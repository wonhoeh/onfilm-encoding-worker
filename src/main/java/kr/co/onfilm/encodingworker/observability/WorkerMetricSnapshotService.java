package kr.co.onfilm.encodingworker.observability;

import kr.co.onfilm.encodingworker.domain.InboxStatus;
import kr.co.onfilm.encodingworker.infra.inbox.MediaEncodeInboxRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

@Component
@RequiredArgsConstructor
public class WorkerMetricSnapshotService {
    private final MediaEncodeInboxRepository repository;
    private final WorkerMediaEncodeMetrics metrics;
    private final Clock clock;

    @Scheduled(fixedDelayString = "${app.worker.metrics-snapshot-delay:30000}")
    @Transactional(readOnly = true)
    public void refresh() {
        for (InboxStatus status : InboxStatus.values()) {
            metrics.updateInboxCount(status, repository.countByStatus(status));
        }

        Instant now = clock.instant();
        Duration oldestFailurePendingAge = repository
                .findOldestUpdatedAtByStatus(InboxStatus.FAILURE_PENDING)
                .map(updatedAt -> Duration.between(updatedAt, now))
                .orElse(Duration.ZERO);
        metrics.updateOldestFailurePendingAge(oldestFailurePendingAge);
    }
}
