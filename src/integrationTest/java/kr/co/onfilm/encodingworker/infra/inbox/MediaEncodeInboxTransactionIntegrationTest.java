package kr.co.onfilm.encodingworker.infra.inbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import kr.co.onfilm.encodingworker.application.FailureCode;
import kr.co.onfilm.encodingworker.application.InboxTransactionService;
import kr.co.onfilm.encodingworker.config.AppProperties;
import kr.co.onfilm.encodingworker.domain.EncodeJobPreset;
import kr.co.onfilm.encodingworker.domain.EncodeJobType;
import kr.co.onfilm.encodingworker.domain.InboxClaim;
import kr.co.onfilm.encodingworker.domain.InboxStatus;
import kr.co.onfilm.encodingworker.domain.MediaEncodeInbox;
import kr.co.onfilm.encodingworker.domain.MediaEncodeRequestedMessage;
import kr.co.onfilm.encodingworker.support.MySqlContainerSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({
        InboxTransactionService.class,
        MediaEncodeInboxTransactionIntegrationTest.TestConfig.class
})
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class MediaEncodeInboxTransactionIntegrationTest extends MySqlContainerSupport {

    private static final Instant NOW = Instant.parse("2026-09-02T01:00:00Z");
    private static final Duration LEASE = Duration.ofMinutes(10);

    @Autowired
    private MediaEncodeInboxRepository repository;

    @Autowired
    private InboxTransactionService transactions;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private ExecutorService executor;

    @BeforeEach
    void resetState() {
        repository.deleteAll();
        executor = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    void shutdownExecutor() throws InterruptedException {
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void commitPersistsInboxAndRuntimeExceptionRollsBackInsert() {
        UUID committedJobId = UUID.randomUUID();
        requiredTransaction().executeWithoutResult(status ->
                repository.saveAndFlush(inbox(committedJobId))
        );

        UUID rolledBackJobId = UUID.randomUUID();
        assertThatThrownBy(() -> requiredTransaction().executeWithoutResult(status -> {
            repository.saveAndFlush(inbox(rolledBackJobId));
            throw new IllegalStateException("injected transaction failure");
        })).isInstanceOf(IllegalStateException.class)
                .hasMessage("injected transaction failure");

        assertThat(repository.existsById(committedJobId)).isTrue();
        assertThat(repository.existsById(rolledBackJobId)).isFalse();
    }

    @Test
    void requiresNewClaimRemainsCommittedWhenOuterTransactionRollsBack() {
        MediaEncodeRequestedMessage message = message();
        UUID outerJobId = UUID.randomUUID();

        requiredTransaction().executeWithoutResult(outerStatus -> {
            assertThat(transactions.claim(message.jobId().toString(), message))
                    .isEqualTo(InboxClaim.PROCESS);
            repository.saveAndFlush(inbox(outerJobId));
            outerStatus.setRollbackOnly();
        });

        assertThat(repository.findById(message.jobId()))
                .get()
                .extracting(MediaEncodeInbox::getStatus)
                .isEqualTo(InboxStatus.PROCESSING);
        assertThat(repository.existsById(outerJobId)).isFalse();
    }

    @Test
    void pessimisticWriteLockBlocksSecondTransactionUntilFirstCommit() throws Exception {
        UUID jobId = UUID.randomUUID();
        requiredTransaction().executeWithoutResult(status ->
                repository.saveAndFlush(inbox(jobId))
        );

        CountDownLatch firstLockAcquired = new CountDownLatch(1);
        CountDownLatch releaseFirstTransaction = new CountDownLatch(1);
        CountDownLatch secondQueryStarted = new CountDownLatch(1);

        Future<Void> first = executor.submit(() -> {
            requiredTransaction().executeWithoutResult(status -> {
                MediaEncodeInbox locked = repository.findByJobIdForUpdate(jobId).orElseThrow();
                locked.recordFailure(
                        FailureCode.ENCODE_FAILED,
                        "first transaction update",
                        true,
                        NOW.plusSeconds(1)
                );
                repository.flush();
                firstLockAcquired.countDown();
                await(releaseFirstTransaction, "first transaction release");
            });
            return null;
        });

        Future<InboxStatus> second = null;
        try {
            assertThat(firstLockAcquired.await(5, TimeUnit.SECONDS)).isTrue();
            second = executor.submit(() -> requiredTransaction().execute(status -> {
                secondQueryStarted.countDown();
                return repository.findByJobIdForUpdate(jobId).orElseThrow().getStatus();
            }));
            assertThat(secondQueryStarted.await(5, TimeUnit.SECONDS)).isTrue();

            Future<InboxStatus> blockedSecond = second;
            assertThatThrownBy(() -> blockedSecond.get(300, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);

            releaseFirstTransaction.countDown();
            assertThat(first.get(5, TimeUnit.SECONDS)).isNull();
            assertThat(second.get(5, TimeUnit.SECONDS)).isEqualTo(InboxStatus.RETRY_WAIT);
        } finally {
            releaseFirstTransaction.countDown();
            if (!first.isDone()) {
                first.cancel(true);
            }
            if (second != null && !second.isDone()) {
                second.cancel(true);
            }
        }

        assertThat(repository.findById(jobId))
                .get()
                .extracting(MediaEncodeInbox::getStatus)
                .isEqualTo(InboxStatus.RETRY_WAIT);
    }

    private TransactionTemplate requiredTransaction() {
        return new TransactionTemplate(transactionManager);
    }

    private MediaEncodeInbox inbox(UUID jobId) {
        return MediaEncodeInbox.begin(
                jobId,
                jobId.toString(),
                "{\"jobId\":\"" + jobId + "\"}",
                NOW,
                LEASE
        );
    }

    private MediaEncodeRequestedMessage message() {
        UUID jobId = UUID.randomUUID();
        UUID requestId = UUID.randomUUID();
        return new MediaEncodeRequestedMessage(
                1,
                jobId,
                requestId,
                "corr-transaction-integration",
                1L,
                2L,
                EncodeJobType.MOVIE,
                EncodeJobPreset.VIDEO_HLS_720P_2500K_AAC_96K,
                "bucket",
                "movie/1/raw/file/" + requestId + ".mp4",
                "bucket",
                "movie/1/file/" + jobId + "/index.m3u8",
                "video/mp4",
                "application/vnd.apple.mpegurl",
                NOW.minusSeconds(10)
        );
    }

    private static void await(CountDownLatch latch, String description) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for " + description);
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for " + description, exception);
        }
    }

    @TestConfiguration
    static class TestConfig {

        @Bean
        Clock clock() {
            return Clock.fixed(NOW, ZoneOffset.UTC);
        }

        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapper().findAndRegisterModules();
        }

        @Bean
        AppProperties appProperties() {
            return new AppProperties(
                    new AppProperties.Worker(
                            "media.encode.requested",
                            "transaction-integration-worker",
                            "ffmpeg",
                            "ffprobe",
                            "/tmp/onfilm-transaction-integration",
                            Duration.ofHours(2),
                            Duration.ofHours(3),
                            10_737_418_240L,
                            Duration.ofHours(6),
                            1,
                            5,
                            1_000,
                            2.0,
                            60_000,
                            60_000
                    ),
                    new AppProperties.Storage(
                            "local",
                            null,
                            "/tmp/onfilm-transaction-integration-storage",
                            Set.of("bucket"),
                            Duration.ofMinutes(10),
                            Duration.ofMinutes(3)
                    ),
                    new AppProperties.CoreApi(
                            URI.create("http://localhost:8080"),
                            "/internal/api/media-jobs/{jobId}/processing",
                            "/internal/api/media-jobs/{jobId}/complete",
                            "/internal/api/media-jobs/{jobId}/fail",
                            "transaction-integration-callback-secret-32-bytes",
                            Duration.ofSeconds(5),
                            Duration.ofSeconds(30)
                    )
            );
        }
    }
}
