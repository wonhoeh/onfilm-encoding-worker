package kr.co.onfilm.encodingworker.interfaces.kafka;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import kr.co.onfilm.encodingworker.application.EncodingJobProcessor;
import kr.co.onfilm.encodingworker.application.FailureReportService;
import kr.co.onfilm.encodingworker.domain.*;
import kr.co.onfilm.encodingworker.observability.WorkerMediaEncodeMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class EncodingRequestedConsumerTest {
    @Mock EncodingJobProcessor processor;
    @Mock FailureReportService failureReportService;

    private SimpleMeterRegistry registry;
    private EncodingRequestedConsumer consumer;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        consumer = new EncodingRequestedConsumer(
                processor,
                failureReportService,
                new WorkerMediaEncodeMetrics(registry)
        );
    }

    @Test
    void recordsInvalidDltMessage() {
        consumer.deadLetter("invalid-key", null);

        assertThat(registry.counter(
                "media.encode.worker.dlt", "result", "invalid").count()).isEqualTo(1);
    }

    @Test
    void recordsDltMessageAndStartsFailureReport() {
        MediaEncodeRequestedMessage message = message();

        consumer.deadLetter(message.jobId().toString(), message);

        verify(failureReportService).prepareAndReportIfEligible(message.jobId());
        assertThat(registry.counter(
                "media.encode.worker.dlt", "result", "received").count()).isEqualTo(1);
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
