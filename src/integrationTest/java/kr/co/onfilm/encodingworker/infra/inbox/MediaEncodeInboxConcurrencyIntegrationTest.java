package kr.co.onfilm.encodingworker.infra.inbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import kr.co.onfilm.encodingworker.application.FailureCode;
import kr.co.onfilm.encodingworker.application.InboxClaimCoordinator;
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
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({
        InboxTransactionService.class,
        InboxClaimCoordinator.class,
        MediaEncodeInboxConcurrencyIntegrationTest.TestConfig.class
})
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class MediaEncodeInboxConcurrencyIntegrationTest extends MySqlContainerSupport {

    private static final Instant NOW = Instant.parse("2026-09-02T03:00:00Z");
    private static final Duration LEASE = Duration.ofMinutes(10);

    @Autowired
    private MediaEncodeInboxRepository repository;

    @Autowired
    private InboxClaimCoordinator coordinator;

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
    void primaryKeyAllowsOnlyOneCommitAfterBothTransactionsObserveNoInbox() throws Exception {
        UUID jobId = UUID.randomUUID();
        CountDownLatch bothChecked = new CountDownLatch(2);
        CountDownLatch startInsert = new CountDownLatch(1);
        Callable<InsertResult> insert = () -> {
            try {
                return requiredTransaction().execute(status -> {
                    boolean alreadyExists = repository.existsById(jobId);
                    bothChecked.countDown();
                    await(startInsert, "concurrent insert start");
                    if (alreadyExists) {
                        return InsertResult.PREEXISTING;
                    }
                    repository.saveAndFlush(inbox(jobId));
                    return InsertResult.COMMITTED;
                });
            } catch (DataIntegrityViolationException exception) {
                assertThat(exception.getMostSpecificCause().getMessage())
                        .contains("PRIMARY");
                return InsertResult.UNIQUE_CONFLICT;
            }
        };

        Future<InsertResult> first = executor.submit(insert);
        Future<InsertResult> second = executor.submit(insert);
        assertThat(bothChecked.await(5, TimeUnit.SECONDS)).isTrue();
        startInsert.countDown();

        assertThat(List.of(
                first.get(10, TimeUnit.SECONDS),
                second.get(10, TimeUnit.SECONDS)
        )).containsExactlyInAnyOrder(
                InsertResult.COMMITTED,
                InsertResult.UNIQUE_CONFLICT
        );
        assertThat(repository.count()).isEqualTo(1);
        assertThat(repository.findById(jobId)).isPresent();
    }

    @Test
    void concurrentDuplicateDeliveryReturnsProcessAndBusyWithOneInboxRow() throws Exception {
        MediaEncodeRequestedMessage message = message();
        CountDownLatch bothReady = new CountDownLatch(2);
        CountDownLatch startClaim = new CountDownLatch(1);
        Callable<InboxClaim> claim = () -> {
            bothReady.countDown();
            await(startClaim, "concurrent claim start");
            return coordinator.claim(message.jobId().toString(), message);
        };

        Future<InboxClaim> first = executor.submit(claim);
        Future<InboxClaim> second = executor.submit(claim);
        assertThat(bothReady.await(5, TimeUnit.SECONDS)).isTrue();
        startClaim.countDown();

        assertThat(List.of(
                first.get(10, TimeUnit.SECONDS),
                second.get(10, TimeUnit.SECONDS)
        )).containsExactlyInAnyOrder(InboxClaim.PROCESS, InboxClaim.BUSY);
        assertThat(repository.count()).isEqualTo(1);
        assertThat(repository.findById(message.jobId()))
                .get()
                .satisfies(inbox -> {
                    assertThat(inbox.getStatus()).isEqualTo(InboxStatus.PROCESSING);
                    assertThat(inbox.getAttempts()).isEqualTo(1);
                });
    }

    @Test
    void optimisticVersionAllowsOnlyOneConcurrentStateUpdate() throws Exception {
        UUID jobId = UUID.randomUUID();
        requiredTransaction().executeWithoutResult(status ->
                repository.saveAndFlush(inbox(jobId))
        );
        CountDownLatch bothLoaded = new CountDownLatch(2);
        CountDownLatch startUpdate = new CountDownLatch(1);

        Future<UpdateResult> retryWait = submitOptimisticUpdate(
                jobId,
                bothLoaded,
                startUpdate,
                inbox -> inbox.recordFailure(
                        FailureCode.SOURCE_DOWNLOAD_FAILED,
                        "concurrent retryable failure",
                        true,
                        NOW.plusSeconds(1)
                )
        );
        Future<UpdateResult> outputUploaded = submitOptimisticUpdate(
                jobId,
                bothLoaded,
                startUpdate,
                inbox -> inbox.outputUploaded(
                        NOW.plusSeconds(1),
                        Duration.ofSeconds(30)
                )
        );
        assertThat(bothLoaded.await(5, TimeUnit.SECONDS)).isTrue();
        startUpdate.countDown();

        assertThat(List.of(
                retryWait.get(10, TimeUnit.SECONDS),
                outputUploaded.get(10, TimeUnit.SECONDS)
        )).containsExactlyInAnyOrder(UpdateResult.COMMITTED, UpdateResult.VERSION_CONFLICT);

        MediaEncodeInbox persisted = repository.findById(jobId).orElseThrow();
        assertThat(persisted.getVersion()).isEqualTo(1L);
        assertThat(persisted.getStatus())
                .isIn(InboxStatus.RETRY_WAIT, InboxStatus.OUTPUT_UPLOADED);
    }

    private Future<UpdateResult> submitOptimisticUpdate(
            UUID jobId,
            CountDownLatch bothLoaded,
            CountDownLatch startUpdate,
            Consumer<MediaEncodeInbox> mutation
    ) {
        return executor.submit(() -> {
            try {
                requiredTransaction().executeWithoutResult(status -> {
                    MediaEncodeInbox loaded = repository.findById(jobId).orElseThrow();
                    bothLoaded.countDown();
                    await(startUpdate, "optimistic update start");
                    mutation.accept(loaded);
                    repository.flush();
                });
                return UpdateResult.COMMITTED;
            } catch (OptimisticLockingFailureException exception) {
                return UpdateResult.VERSION_CONFLICT;
            }
        });
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
                "corr-concurrency-integration",
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

    private enum InsertResult {
        COMMITTED,
        UNIQUE_CONFLICT,
        PREEXISTING
    }

    private enum UpdateResult {
        COMMITTED,
        VERSION_CONFLICT
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
                            "concurrency-integration-worker",
                            "ffmpeg",
                            "ffprobe",
                            "/tmp/onfilm-concurrency-integration",
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
                            "/tmp/onfilm-concurrency-integration-storage",
                            Set.of("bucket"),
                            Duration.ofMinutes(10),
                            Duration.ofMinutes(3)
                    ),
                    new AppProperties.CoreApi(
                            URI.create("http://localhost:8080"),
                            "/internal/api/media-jobs/{jobId}/processing",
                            "/internal/api/media-jobs/{jobId}/complete",
                            "/internal/api/media-jobs/{jobId}/fail",
                            "concurrency-integration-callback-secret-32-bytes",
                            Duration.ofSeconds(5),
                            Duration.ofSeconds(30)
                    )
            );
        }
    }
}
