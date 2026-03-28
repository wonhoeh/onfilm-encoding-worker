package kr.co.onfilm.encodingworker.interfaces.kafka;

import jakarta.validation.Valid;
import kr.co.onfilm.encodingworker.application.EncodingJobProcessor;
import kr.co.onfilm.encodingworker.domain.MediaEncodeRequestedMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

@Slf4j
@Validated
@Component
@RequiredArgsConstructor
public class EncodingRequestedConsumer {

    private final EncodingJobProcessor processor;

    @KafkaListener(
            topics = "${app.worker.topic}",
            groupId = "${app.worker.group-id}"
    )
    @RetryableTopic(
            attempts = "5",
            backoff = @Backoff(delay = 1000, multiplier = 2),
            dltTopicSuffix = ".dlt"
    )
    public void consume(@Valid @Payload MediaEncodeRequestedMessage message) {
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
        processor.process(message);
    }
}
