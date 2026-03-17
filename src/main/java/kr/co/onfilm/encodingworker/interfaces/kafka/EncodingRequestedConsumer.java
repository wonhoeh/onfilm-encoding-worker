package kr.co.onfilm.encodingworker.interfaces.kafka;

import jakarta.validation.Valid;
import kr.co.onfilm.encodingworker.application.EncodingJobProcessor;
import kr.co.onfilm.encodingworker.domain.MediaEncodeRequestedMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
<<<<<<< HEAD
import org.springframework.messaging.handler.annotation.Payload;
=======
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
>>>>>>> a4d4e61 (feat: 로컬에서 인코딩 테스트할 수 있는 환경 구성 및 문서 작업)
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
<<<<<<< HEAD
    public void consume(@Valid @Payload MediaEncodeRequestedMessage message) {
        log.info("Consumed encode job. jobId={}, type={}, preset={}", message.jobId(), message.jobType(), message.preset());
=======
    @RetryableTopic(
            // 총 시도 횟수 (최초 시도 1회 + 재시도 4회)
            attempts = "5",

            // 재시도 간격 (1000ms -> 2000ms -> 4000ms -> 8000ms 순으로 재시도 시간이 증가한다.)
            backoff = @Backoff(delay = 1000, multiplier = 2),

            // DLT 토픽 이름에 붙일 접미사
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
>>>>>>> a4d4e61 (feat: 로컬에서 인코딩 테스트할 수 있는 환경 구성 및 문서 작업)
        processor.process(message);
    }
}
