package kr.co.onfilm.encodingworker.interfaces.kafka;

import kr.co.onfilm.encodingworker.application.EncodingJobProcessor;
import kr.co.onfilm.encodingworker.application.FailureReportService;
import kr.co.onfilm.encodingworker.application.PermanentEncodingException;
import kr.co.onfilm.encodingworker.domain.MediaEncodeRequestedMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

@Slf4j
@Validated
@Component
@RequiredArgsConstructor
public class EncodingRequestedConsumer {

    private final EncodingJobProcessor processor;
    private final FailureReportService failureReportService;

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
        log.info(
                "Consumed encode job. jobId={}, movieId={}, type={}, preset={}, source={}/{}, target={}/{}",
                message.jobId(),
                message.movieId(),
                message.jobType(),
                message.preset(),
                message.sourceBucket(),
                message.sourceKey(),
                message.targetBucket(),
                message.targetKey()
        );
        processor.process(kafkaKey, message);
    }

    @DltHandler
    public void deadLetter(
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String kafkaKey,
            @Payload MediaEncodeRequestedMessage message
    ) {
        if (message == null || message.jobId() == null) {
            log.error("Invalid encode message reached DLT without a jobId. kafkaKey={}", kafkaKey);
            return;
        }
        log.error("Encode job reached DLT. jobId={}, kafkaKey={}", message.jobId(), kafkaKey);
        failureReportService.prepareAndReportIfEligible(message.jobId());
    }
}
