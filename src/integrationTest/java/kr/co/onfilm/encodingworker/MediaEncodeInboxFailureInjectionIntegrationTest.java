package kr.co.onfilm.encodingworker;

import com.fasterxml.jackson.databind.ObjectMapper;
import kr.co.onfilm.encodingworker.application.FailureCode;
import kr.co.onfilm.encodingworker.application.InboxClaimCoordinator;
import kr.co.onfilm.encodingworker.application.InboxTransactionService;
import kr.co.onfilm.encodingworker.application.PermanentEncodingException;
import kr.co.onfilm.encodingworker.config.AppProperties;
import kr.co.onfilm.encodingworker.domain.EncodeJobPreset;
import kr.co.onfilm.encodingworker.domain.EncodeJobType;
import kr.co.onfilm.encodingworker.domain.InboxClaim;
import kr.co.onfilm.encodingworker.domain.InboxStatus;
import kr.co.onfilm.encodingworker.domain.MediaEncodeInbox;
import kr.co.onfilm.encodingworker.domain.MediaEncodeRequestedMessage;
import kr.co.onfilm.encodingworker.infra.inbox.MediaEncodeInboxRepository;
import kr.co.onfilm.encodingworker.support.MySqlContainerSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({
        InboxTransactionService.class,
        InboxClaimCoordinator.class,
        MediaEncodeInboxFailureInjectionIntegrationTest.TestConfig.class
})
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class MediaEncodeInboxFailureInjectionIntegrationTest extends MySqlContainerSupport {

    private static final Instant NOW = Instant.parse("2026-08-31T00:00:00Z");

    @Autowired
    private InboxClaimCoordinator coordinator;

    @Autowired
    private InboxTransactionService transactions;

    @Autowired
    private MediaEncodeInboxRepository repository;

    @Autowired
    private MutableClock clock;

    @Autowired
    private AppProperties properties;

    @BeforeEach
    void resetState() {
        repository.deleteAll();
        clock.set(NOW);
    }

    @Test
    void duplicateMessageIsBusyUntilLeaseExpiresThenCrashRecoveryCanProcessIt() {
        MediaEncodeRequestedMessage message = message();

        assertThat(coordinator.claim(message.jobId().toString(), message))
                .isEqualTo(InboxClaim.PROCESS);
        assertThat(coordinator.claim(message.jobId().toString(), message))
                .isEqualTo(InboxClaim.BUSY);

        clock.advance(properties.worker().processingLease().plusSeconds(1));

        assertThat(coordinator.claim(message.jobId().toString(), message))
                .isEqualTo(InboxClaim.PROCESS);
        MediaEncodeInbox recovered = repository.findById(message.jobId()).orElseThrow();
        assertThat(recovered.getStatus()).isEqualTo(InboxStatus.PROCESSING);
        assertThat(recovered.getAttempts()).isEqualTo(2);
    }

    @Test
    void callbackFailureAfterUploadResumesCallbackOnlyWithoutReencoding() {
        MediaEncodeRequestedMessage message = message();
        assertThat(coordinator.claim(message.jobId().toString(), message))
                .isEqualTo(InboxClaim.PROCESS);
        transactions.markOutputUploaded(message.jobId());

        transactions.recordFailure(
                message.jobId(), FailureCode.CORE_API_UNAVAILABLE,
                "injected callback timeout", true
        );

        MediaEncodeInbox failedCallback = repository.findById(message.jobId()).orElseThrow();
        assertThat(failedCallback.getStatus()).isEqualTo(InboxStatus.OUTPUT_UPLOADED);
        assertThat(failedCallback.getLeaseUntil()).isNull();
        assertThat(coordinator.claim(message.jobId().toString(), message))
                .isEqualTo(InboxClaim.CALLBACK_ONLY);

        transactions.markDone(message.jobId());
        assertThat(coordinator.claim(message.jobId().toString(), message))
                .isEqualTo(InboxClaim.TERMINAL);
        assertThat(repository.findById(message.jobId()).orElseThrow().getStatus())
                .isEqualTo(InboxStatus.DONE);
    }

    @Test
    void sameJobIdWithDifferentPayloadIsRejectedWithoutChangingOriginalInbox() {
        MediaEncodeRequestedMessage original = message();
        MediaEncodeRequestedMessage conflicting = new MediaEncodeRequestedMessage(
                original.schemaVersion(), original.jobId(), original.requestId(), original.correlationId(),
                999L, original.requestedByUserId(), original.jobType(), original.preset(),
                original.sourceBucket(), original.sourceKey(), original.targetBucket(), original.targetKey(),
                original.sourceContentType(), original.targetContentType(), original.requestedAt()
        );
        assertThat(coordinator.claim(original.jobId().toString(), original))
                .isEqualTo(InboxClaim.PROCESS);

        assertThatThrownBy(() -> coordinator.claim(conflicting.jobId().toString(), conflicting))
                .isInstanceOf(PermanentEncodingException.class)
                .hasMessage("Same jobId was received with a different request");

        MediaEncodeInbox inbox = repository.findById(original.jobId()).orElseThrow();
        assertThat(inbox.getStatus()).isEqualTo(InboxStatus.PROCESSING);
        assertThat(inbox.getAttempts()).isEqualTo(1);
    }

    @Test
    void exhaustedFailureRemainsTerminalAndCannotBeReprocessedByDuplicateMessage() {
        MediaEncodeRequestedMessage message = message();
        assertThat(coordinator.claim(message.jobId().toString(), message))
                .isEqualTo(InboxClaim.PROCESS);
        transactions.recordFailure(
                message.jobId(), FailureCode.ENCODE_FAILED,
                "injected permanent ffmpeg failure", false
        );
        transactions.markFailed(message.jobId());

        assertThat(coordinator.claim(message.jobId().toString(), message))
                .isEqualTo(InboxClaim.TERMINAL);
        MediaEncodeInbox terminal = repository.findById(message.jobId()).orElseThrow();
        assertThat(terminal.getStatus()).isEqualTo(InboxStatus.FAILED);
        assertThat(terminal.getFailureCode()).isEqualTo(FailureCode.ENCODE_FAILED);
        assertThat(terminal.getFailureReason()).isEqualTo("injected permanent ffmpeg failure");
    }

    private MediaEncodeRequestedMessage message() {
        UUID jobId = UUID.randomUUID();
        UUID requestId = UUID.randomUUID();
        return new MediaEncodeRequestedMessage(
                1, jobId, requestId, "corr-failure-injection", 1L, 2L,
                EncodeJobType.MOVIE, EncodeJobPreset.VIDEO_HLS_720P_2500K_AAC_96K,
                "bucket", "movie/1/raw/file/" + requestId + ".mp4",
                "bucket", "movie/1/file/" + jobId + "/index.m3u8",
                "video/mp4", "application/vnd.apple.mpegurl", NOW.minusSeconds(10)
        );
    }

    @TestConfiguration
    static class TestConfig {

        @Bean
        MutableClock clock() {
            return new MutableClock(NOW);
        }

        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapper().findAndRegisterModules();
        }

        @Bean
        AppProperties appProperties() {
            return new AppProperties(
                    new AppProperties.Worker(
                            "media.encode.requested", "failure-injection-worker",
                            "ffmpeg", "ffprobe", "/tmp/onfilm-failure-injection",
                            Duration.ofHours(2), Duration.ofHours(3), 10_737_418_240L,
                            Duration.ofHours(6), 1, 5, 1_000, 2.0, 60_000, 60_000
                    ),
                    new AppProperties.Storage(
                            "local", null, "/tmp/onfilm-failure-injection-storage",
                            Set.of("bucket"), Duration.ofMinutes(10), Duration.ofMinutes(3)
                    ),
                    new AppProperties.CoreApi(
                            URI.create("http://localhost:8080"),
                            "/internal/api/media-jobs/{jobId}/processing",
                            "/internal/api/media-jobs/{jobId}/complete",
                            "/internal/api/media-jobs/{jobId}/fail",
                            "failure-injection-callback-secret-32-bytes",
                            Duration.ofSeconds(5), Duration.ofSeconds(30)
                    )
            );
        }
    }

    static final class MutableClock extends Clock {
        private Instant instant;

        private MutableClock(Instant instant) {
            this.instant = instant;
        }

        void set(Instant instant) {
            this.instant = instant;
        }

        void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
