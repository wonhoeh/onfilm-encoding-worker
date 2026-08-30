package kr.co.onfilm.encodingworker.interfaces.kafka;

import kr.co.onfilm.encodingworker.application.EncodingJobProcessor;
import kr.co.onfilm.encodingworker.application.FailureReportService;
import kr.co.onfilm.encodingworker.application.PermanentEncodingException;
import kr.co.onfilm.encodingworker.domain.MediaEncodeRequestedMessage;
import kr.co.onfilm.encodingworker.observability.CorrelationIdContext;
import kr.co.onfilm.encodingworker.observability.WorkerMediaEncodeMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.retry.annotation.Backoff;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

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
            @Payload MediaEncodeRequestedMessage message
    ) {
        if (message == null || message.jobId() == null) {
            metrics.recordDlt("invalid");
            log.error("Invalid media encode message reached DLT. {} {} {}",
                    kv("eventType", "MEDIA_ENCODE_DLT_INVALID_MESSAGE"),
                    kv("kafkaKey", kafkaKey),
                    kv("status", "DLT"));
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
            log.error("Media encode job reached DLT. {} {} {}",
                    kv("eventType", "MEDIA_ENCODE_DLT_RECEIVED"),
                    kv("kafkaKey", kafkaKey),
                    kv("status", "DLT"));
            failureReportService.prepareAndReportIfEligible(message.jobId());
        }
    }
}
