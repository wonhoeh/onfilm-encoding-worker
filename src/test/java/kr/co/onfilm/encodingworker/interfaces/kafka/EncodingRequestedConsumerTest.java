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
import org.springframework.kafka.support.KafkaHeaders;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Map;
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
        consumer.deadLetter("invalid-key", null, Map.of());

        assertThat(registry.counter(
                "media.encode.worker.dlt", "result", "invalid").count()).isEqualTo(1);
    }

    @Test
    void recordsDltMessageAndStartsFailureReport() {
        MediaEncodeRequestedMessage message = message();

        consumer.deadLetter(message.jobId().toString(), message, dltHeaders());

        verify(failureReportService).prepareAndReportIfEligible(message.jobId());
        assertThat(registry.counter(
                "media.encode.worker.dlt", "result", "received").count()).isEqualTo(1);
    }

    @Test
    void extractsDltLocationAndSanitizesFailureMetadata() {
        EncodingRequestedConsumer.DltRecordMetadata metadata =
                EncodingRequestedConsumer.DltRecordMetadata.from(dltHeaders());

        assertThat(metadata.dltTopic()).isEqualTo("media.encode.requested.dlt");
        assertThat(metadata.dltPartition()).isEqualTo("2");
        assertThat(metadata.dltOffset()).isEqualTo("41");
        assertThat(metadata.originalTopic()).isEqualTo("media.encode.requested");
        assertThat(metadata.originalPartition()).isEqualTo("1");
        assertThat(metadata.originalOffset()).isEqualTo("37");
        assertThat(metadata.failureType()).isEqualTo("java.lang.IllegalStateException");
        assertThat(metadata.failureMessage())
                .isEqualTo("callback token=[REDACTED] failed");
    }

    private Map<String, Object> dltHeaders() {
        return Map.of(
                KafkaHeaders.RECEIVED_TOPIC, "media.encode.requested.dlt",
                KafkaHeaders.RECEIVED_PARTITION, 2,
                KafkaHeaders.OFFSET, 41L,
                KafkaHeaders.DLT_ORIGINAL_TOPIC, "media.encode.requested",
                KafkaHeaders.DLT_ORIGINAL_PARTITION, ByteBuffer.allocate(4).putInt(1).array(),
                KafkaHeaders.DLT_ORIGINAL_OFFSET, ByteBuffer.allocate(8).putLong(37L).array(),
                KafkaHeaders.DLT_EXCEPTION_FQCN, "java.lang.IllegalStateException",
                KafkaHeaders.DLT_EXCEPTION_MESSAGE, "callback token=secret-value failed"
        );
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
