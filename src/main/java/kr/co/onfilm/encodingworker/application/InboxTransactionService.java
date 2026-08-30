package kr.co.onfilm.encodingworker.application;

import kr.co.onfilm.encodingworker.config.AppProperties;
import kr.co.onfilm.encodingworker.domain.*;
import kr.co.onfilm.encodingworker.infra.inbox.MediaEncodeInboxRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

@Service
@RequiredArgsConstructor
public class InboxTransactionService {
    private final MediaEncodeInboxRepository repository;
    private final AppProperties properties;
    private final Clock clock;
    private final ObjectMapper objectMapper;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public InboxClaim claim(String kafkaKey, MediaEncodeRequestedMessage message) {
        String payload = serialize(message);
        return repository.findByJobIdForUpdate(message.jobId())
                .map(inbox -> {
                    if (!inbox.hasSameRequest(kafkaKey, payload)) {
                        throw new PermanentEncodingException(
                                FailureCode.INVALID_REQUEST,
                                "Same jobId was received with a different request"
                        );
                    }
                    return inbox.claim(clock.instant(), properties.worker().processingLease());
                })
                .orElseGet(() -> {
                    repository.saveAndFlush(MediaEncodeInbox.begin(
                            message.jobId(), kafkaKey, payload,
                            clock.instant(), properties.worker().processingLease()));
                    return InboxClaim.PROCESS;
                });
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markOutputUploaded(UUID jobId) {
        find(jobId).outputUploaded(
                clock.instant(), properties.coreApi().readTimeout().multipliedBy(2));
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markDone(UUID jobId) {
        find(jobId).done(clock.instant());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recordFailure(UUID jobId, FailureCode code, String reason, boolean retryable) {
        find(jobId).recordFailure(code, reason, retryable, clock.instant());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void prepareFailureReport(UUID jobId) {
        find(jobId).prepareFailureReport(clock.instant());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markFailed(UUID jobId) {
        find(jobId).failed(clock.instant());
    }

    @Transactional(readOnly = true)
    public Optional<FailureSnapshot> failure(UUID jobId) {
        return repository.findById(jobId)
                .map(inbox -> {
                    MediaEncodeRequestedMessage message = deserialize(inbox.getPayload());
                    return new FailureSnapshot(
                            inbox.getFailureCode(),
                            inbox.getFailureReason(),
                            inbox.getStatus(),
                            message.correlationId(),
                            message.requestId()
                    );
                });
    }

    @Transactional(readOnly = true)
    public List<UUID> pendingFailureReports() {
        return repository.findTop100ByStatusOrderByUpdatedAt(InboxStatus.FAILURE_PENDING)
                .stream().map(MediaEncodeInbox::getJobId).toList();
    }

    @Transactional(readOnly = true)
    public List<RecoverySnapshot> staleProcessingJobs() {
        List<MediaEncodeInbox> stale = new ArrayList<>(repository.findTop100ByStatusAndLeaseUntilBeforeOrderByUpdatedAt(
                        InboxStatus.PROCESSING, clock.instant())
        );
        stale.addAll(repository.findTop100ByStatusAndLeaseUntilBeforeOrderByUpdatedAt(
                InboxStatus.OUTPUT_UPLOADED, clock.instant()));
        return stale.stream()
                .map(inbox -> new RecoverySnapshot(
                        inbox.getKafkaKey(), deserialize(inbox.getPayload())))
                .toList();
    }

    private MediaEncodeInbox find(UUID jobId) {
        return repository.findByJobIdForUpdate(jobId)
                .orElseThrow(() -> new IllegalStateException("INBOX_JOB_NOT_FOUND"));
    }

    public record FailureSnapshot(
            FailureCode code,
            String reason,
            InboxStatus status,
            String correlationId,
            UUID requestId
    ) {
    }

    public record RecoverySnapshot(String kafkaKey, MediaEncodeRequestedMessage message) {
    }

    private String serialize(MediaEncodeRequestedMessage message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to serialize inbox payload", exception);
        }
    }

    private MediaEncodeRequestedMessage deserialize(String payload) {
        try {
            return objectMapper.readValue(payload, MediaEncodeRequestedMessage.class);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to deserialize inbox payload", exception);
        }
    }
}
