package kr.co.onfilm.encodingworker.interfaces.kafka;

import jakarta.validation.Valid;
import kr.co.onfilm.encodingworker.application.EncodingJobProcessor;
import kr.co.onfilm.encodingworker.domain.MediaEncodeRequestedMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
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
    public void consume(@Valid @Payload MediaEncodeRequestedMessage message) {
        log.info("Consumed encode job. jobId={}, type={}, preset={}", message.jobId(), message.jobType(), message.preset());
        processor.process(message);
    }
}
