package kr.co.onfilm.encodingworker.interfaces.kafka;

import kr.co.onfilm.encodingworker.application.EncodingJobProcessor;
import kr.co.onfilm.encodingworker.application.FailureReportService;
import kr.co.onfilm.encodingworker.application.PermanentEncodingException;
import kr.co.onfilm.encodingworker.domain.MediaEncodeRequestedMessage;
import kr.co.onfilm.encodingworker.observability.CorrelationIdContext;
import kr.co.onfilm.encodingworker.observability.WorkerMediaEncodeMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static net.logstash.logback.argument.StructuredArguments.kv;

@Slf4j
@Validated
@Component
@RequiredArgsConstructor
public class EncodingRequestedConsumer {

    private final EncodingJobProcessor processor;
    private final FailureReportService failureReportService;
    private final WorkerMediaEncodeMetrics metrics;

    @KafkaListener(
            topics = "${app.worker.topic}",
            groupId = "${app.worker.group-id}",
            concurrency = "${app.worker.concurrency:1}"
    )
    @RetryableTopic(
            attempts = "${app.worker.retry-attempts:5}",
            backoff = @Backoff(
                    delayExpression = "${app.worker.retry-delay-millis:1000}",
                    multiplierExpression = "${app.worker.retry-multiplier:2}"
            ),
            exclude = PermanentEncodingException.class,
            dltTopicSuffix = ".dlt"
    )
    public void consume(
            @Header(KafkaHeaders.RECEIVED_KEY) String kafkaKey,
            @Payload MediaEncodeRequestedMessage message
    ) {
        try (MDC.MDCCloseable ignoredCorrelation = MDC.putCloseable(
                     CorrelationIdContext.MDC_KEY,
                     CorrelationIdContext.resolve(message.correlationId(), message.requestId())
             );
             MDC.MDCCloseable ignoredJob = MDC.putCloseable("jobId", message.jobId().toString());
             MDC.MDCCloseable ignoredRequest = MDC.putCloseable("requestId", message.requestId().toString())) {
            log.info("Media encode message consumed. {} {} {} {}",
                    kv("eventType", "MEDIA_ENCODE_MESSAGE_CONSUMED"),
                    kv("movieId", message.movieId()),
                    kv("jobType", message.jobType()),
                    kv("preset", message.preset()));
            processor.process(kafkaKey, message);
        }
    }

    @DltHandler
    public void deadLetter(
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String kafkaKey,
            @Payload MediaEncodeRequestedMessage message,
            @Headers Map<String, Object> headers
    ) {
        DltRecordMetadata metadata = DltRecordMetadata.from(headers);
        if (message == null || message.jobId() == null) {
            metrics.recordDlt("invalid");
            log.error("Invalid media encode message reached DLT. {} {} {} {} {} {} {} {} {}",
                    kv("eventType", "MEDIA_ENCODE_DLT_INVALID_MESSAGE"),
                    kv("kafkaKey", kafkaKey),
                    kv("status", "DLT"),
                    kv("dltTopic", metadata.dltTopic()),
                    kv("dltPartition", metadata.dltPartition()),
                    kv("dltOffset", metadata.dltOffset()),
                    kv("originalTopic", metadata.originalTopic()),
                    kv("originalPartition", metadata.originalPartition()),
                    kv("originalOffset", metadata.originalOffset()));
            return;
        }
        metrics.recordDlt("received");
        String requestId = message.requestId() == null ? "-" : message.requestId().toString();
        try (MDC.MDCCloseable ignoredCorrelation = MDC.putCloseable(
                     CorrelationIdContext.MDC_KEY,
                     CorrelationIdContext.resolve(message.correlationId(), message.requestId())
             );
             MDC.MDCCloseable ignoredJob = MDC.putCloseable("jobId", message.jobId().toString());
             MDC.MDCCloseable ignoredRequest = MDC.putCloseable("requestId", requestId)) {
            log.error("Media encode job reached DLT. {} {} {} {} {} {} {} {} {} {} {}",
                    kv("eventType", "MEDIA_ENCODE_DLT_RECEIVED"),
                    kv("kafkaKey", kafkaKey),
                    kv("status", "DLT"),
                    kv("dltTopic", metadata.dltTopic()),
                    kv("dltPartition", metadata.dltPartition()),
                    kv("dltOffset", metadata.dltOffset()),
                    kv("originalTopic", metadata.originalTopic()),
                    kv("originalPartition", metadata.originalPartition()),
                    kv("originalOffset", metadata.originalOffset()),
                    kv("failureType", metadata.failureType()),
                    kv("failureMessage", metadata.failureMessage()));
            failureReportService.prepareAndReportIfEligible(message.jobId());
        }
    }

    record DltRecordMetadata(
            String dltTopic,
            String dltPartition,
            String dltOffset,
            String originalTopic,
            String originalPartition,
            String originalOffset,
            String failureType,
            String failureMessage
    ) {
        private static final String UNKNOWN = "-";
        private static final int MAX_FAILURE_MESSAGE_LENGTH = 500;

        static DltRecordMetadata from(Map<String, Object> headers) {
            Map<String, Object> safeHeaders = headers == null ? Map.of() : headers;
            return new DltRecordMetadata(
                    text(safeHeaders, KafkaHeaders.RECEIVED_TOPIC),
                    number(safeHeaders, KafkaHeaders.RECEIVED_PARTITION),
                    number(safeHeaders, KafkaHeaders.OFFSET),
                    text(safeHeaders, KafkaHeaders.DLT_ORIGINAL_TOPIC, KafkaHeaders.ORIGINAL_TOPIC),
                    number(safeHeaders, KafkaHeaders.DLT_ORIGINAL_PARTITION, KafkaHeaders.ORIGINAL_PARTITION),
                    number(safeHeaders, KafkaHeaders.DLT_ORIGINAL_OFFSET, KafkaHeaders.ORIGINAL_OFFSET),
                    text(safeHeaders, KafkaHeaders.DLT_EXCEPTION_FQCN, KafkaHeaders.EXCEPTION_FQCN),
                    sanitize(text(safeHeaders,
                            KafkaHeaders.DLT_EXCEPTION_MESSAGE,
                            KafkaHeaders.EXCEPTION_MESSAGE))
            );
        }

        private static String text(Map<String, Object> headers, String... keys) {
            Object value = first(headers, keys);
            if (value == null) return UNKNOWN;
            if (value instanceof byte[] bytes) {
                return new String(bytes, StandardCharsets.UTF_8);
            }
            return value.toString();
        }

        private static String number(Map<String, Object> headers, String... keys) {
            Object value = first(headers, keys);
            if (value == null) return UNKNOWN;
            if (value instanceof Number number) return number.toString();
            if (value instanceof byte[] bytes) {
                if (bytes.length == Integer.BYTES) {
                    return Integer.toString(ByteBuffer.wrap(bytes).getInt());
                }
                if (bytes.length == Long.BYTES) {
                    return Long.toString(ByteBuffer.wrap(bytes).getLong());
                }
                return new String(bytes, StandardCharsets.UTF_8);
            }
            return value.toString();
        }

        private static Object first(Map<String, Object> headers, String... keys) {
            for (String key : keys) {
                Object value = headers.get(key);
                if (value != null) return value;
            }
            return null;
        }

        private static String sanitize(String message) {
            if (UNKNOWN.equals(message)) return message;
            String sanitized = message
                    .replaceAll("(?i)(authorization|token|secret|password)=[^\\s,]+", "$1=[REDACTED]")
                    .replaceAll("[\\r\\n]+", " ");
            return sanitized.length() <= MAX_FAILURE_MESSAGE_LENGTH
                    ? sanitized
                    : sanitized.substring(0, MAX_FAILURE_MESSAGE_LENGTH);
        }
    }
}
