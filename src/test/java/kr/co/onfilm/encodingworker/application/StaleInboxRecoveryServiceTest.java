package kr.co.onfilm.encodingworker.application;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import kr.co.onfilm.encodingworker.domain.*;
import kr.co.onfilm.encodingworker.observability.WorkerMediaEncodeMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class StaleInboxRecoveryServiceTest {
    @Mock InboxTransactionService transactions;
    @Mock EncodingJobProcessor processor;

    @Test
    void recordsSuccessfulStaleRecovery() {
        MediaEncodeRequestedMessage message = message();
        given(transactions.staleProcessingJobs()).willReturn(List.of(
                new InboxTransactionService.RecoverySnapshot(message.jobId().toString(), message)
        ));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        StaleInboxRecoveryService service = new StaleInboxRecoveryService(
                transactions,
                processor,
                new WorkerMediaEncodeMetrics(registry)
        );

        service.recover();

        verify(processor).process(message.jobId().toString(), message);
        assertThat(registry.counter(
                "media.encode.worker.stale.recovery", "result", "started").count())
                .isEqualTo(1);
        assertThat(registry.counter(
                "media.encode.worker.stale.recovery", "result", "success").count())
                .isEqualTo(1);
    }

    private MediaEncodeRequestedMessage message() {
        UUID jobId = UUID.randomUUID();
        UUID requestId = UUID.randomUUID();
        return new MediaEncodeRequestedMessage(
                1, jobId, requestId, "corr-123", 1L, 2L,
                EncodeJobType.MOVIE, EncodeJobPreset.VIDEO_HLS_720P_2500K_AAC_96K,
                "bucket", "movie/1/raw/file/" + requestId + ".mp4",
                "bucket", "movie/1/file/" + jobId + "/index.m3u8",
                "video/mp4", "application/vnd.apple.mpegurl",
                Instant.parse("2026-08-30T00:00:00Z")
        );
    }
}
