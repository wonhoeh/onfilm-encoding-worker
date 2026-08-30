package kr.co.onfilm.encodingworker.observability;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import kr.co.onfilm.encodingworker.domain.InboxStatus;
import kr.co.onfilm.encodingworker.infra.inbox.MediaEncodeInboxRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
class WorkerMetricSnapshotServiceTest {
    private static final Instant NOW = Instant.parse("2026-08-30T00:00:00Z");

    @Mock MediaEncodeInboxRepository repository;

    private SimpleMeterRegistry registry;
    private WorkerMetricSnapshotService service;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        service = new WorkerMetricSnapshotService(
                repository,
                new WorkerMediaEncodeMetrics(registry),
                Clock.fixed(NOW, ZoneOffset.UTC)
        );
    }

    @Test
    void refreshesInboxStateAndFailurePendingAgeGauges() {
        given(repository.countByStatus(any())).willAnswer(invocation -> switch (
                invocation.<InboxStatus>getArgument(0)
        ) {
            case PROCESSING -> 2L;
            case FAILURE_PENDING -> 3L;
            case DONE -> 10L;
            default -> 0L;
        });
        given(repository.findOldestUpdatedAtByStatus(InboxStatus.FAILURE_PENDING))
                .willReturn(Optional.of(NOW.minusSeconds(180)));

        service.refresh();

        assertThat(gauge("media.encode.worker.inbox.records", "status", "processing"))
                .isEqualTo(2);
        assertThat(gauge("media.encode.worker.inbox.records", "status", "failure_pending"))
                .isEqualTo(3);
        assertThat(gauge("media.encode.worker.inbox.records", "status", "done"))
                .isEqualTo(10);
        assertThat(gauge(
                "media.encode.worker.inbox.oldest.failure.pending.age", null, null
        )).isEqualTo(180);
    }

    private double gauge(String name, String tagKey, String tagValue) {
        var search = registry.get(name);
        if (tagKey != null) {
            search = search.tag(tagKey, tagValue);
        }
        return search.gauge().value();
    }
}
