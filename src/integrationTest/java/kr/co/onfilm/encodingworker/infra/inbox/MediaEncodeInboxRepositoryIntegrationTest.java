package kr.co.onfilm.encodingworker.infra.inbox;

import jakarta.persistence.EntityManager;
import kr.co.onfilm.encodingworker.application.FailureCode;
import kr.co.onfilm.encodingworker.domain.InboxStatus;
import kr.co.onfilm.encodingworker.domain.MediaEncodeInbox;
import kr.co.onfilm.encodingworker.support.MySqlContainerSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class MediaEncodeInboxRepositoryIntegrationTest extends MySqlContainerSupport {

    private static final Instant BASE_TIME = Instant.parse("2026-09-02T00:00:00Z");
    private static final Duration PROCESSING_LEASE = Duration.ofMinutes(10);

    @Autowired
    private MediaEncodeInboxRepository repository;

    @Autowired
    private EntityManager entityManager;

    @BeforeEach
    void resetState() {
        repository.deleteAll();
        repository.flush();
    }

    @Test
    void persistsLoadsAndDeletesInboxUsingUuidStringId() {
        UUID jobId = UUID.randomUUID();
        String payload = "{\"content\":\"" + "x".repeat(1_024) + "\"}";
        repository.saveAndFlush(MediaEncodeInbox.begin(
                jobId,
                jobId.toString(),
                payload,
                BASE_TIME,
                PROCESSING_LEASE
        ));
        entityManager.clear();

        MediaEncodeInbox reloaded = repository.findById(jobId).orElseThrow();
        assertThat(reloaded.getJobId()).isEqualTo(jobId);
        assertThat(reloaded.getKafkaKey()).isEqualTo(jobId.toString());
        assertThat(reloaded.getPayload()).isEqualTo(payload);
        assertThat(reloaded.getStatus()).isEqualTo(InboxStatus.PROCESSING);
        assertThat(reloaded.getVersion()).isNotNull();
        assertThat(reloaded.getFailureCode()).isNull();
        assertThat(reloaded.getFailureReason()).isNull();

        repository.delete(reloaded);
        repository.flush();
        entityManager.clear();

        assertThat(repository.findById(jobId)).isEmpty();
    }

    @Test
    void countsEachStatusAndFindsOldestFailurePendingTime() {
        Instant oldestFailureTime = BASE_TIME.plusSeconds(10);
        Instant newestFailureTime = BASE_TIME.plusSeconds(30);
        repository.saveAllAndFlush(List.of(
                processing(BASE_TIME),
                failurePending(newestFailureTime),
                failurePending(oldestFailureTime),
                done(BASE_TIME.plusSeconds(40))
        ));
        entityManager.clear();

        assertThat(repository.countByStatus(InboxStatus.PROCESSING)).isEqualTo(1);
        assertThat(repository.countByStatus(InboxStatus.FAILURE_PENDING)).isEqualTo(2);
        assertThat(repository.countByStatus(InboxStatus.DONE)).isEqualTo(1);
        assertThat(repository.countByStatus(InboxStatus.FAILED)).isZero();
        assertThat(repository.findOldestUpdatedAtByStatus(InboxStatus.FAILURE_PENDING))
                .contains(oldestFailureTime);
        assertThat(repository.findOldestUpdatedAtByStatus(InboxStatus.FAILED)).isEmpty();
    }

    @Test
    void pendingFailureQueryReturnsOnlyOldestHundredInOrder() {
        List<MediaEncodeInbox> pending = new ArrayList<>();
        List<Instant> expectedTimes = new ArrayList<>();
        for (int index = 0; index < 105; index++) {
            Instant updatedAt = BASE_TIME.plusSeconds(index);
            pending.add(failurePending(updatedAt));
            if (index < 100) {
                expectedTimes.add(updatedAt);
            }
        }
        repository.saveAllAndFlush(pending);
        entityManager.clear();

        List<MediaEncodeInbox> result = repository.findTop100ByStatusOrderByUpdatedAt(
                InboxStatus.FAILURE_PENDING
        );

        assertThat(result).hasSize(100);
        assertThat(result)
                .extracting(MediaEncodeInbox::getUpdatedAt)
                .containsExactlyElementsOf(expectedTimes);
    }

    @Test
    void expiredLeaseQueryFiltersByStatusAndUsesStrictBoundary() {
        Instant threshold = BASE_TIME.plus(Duration.ofHours(1));
        MediaEncodeInbox oldestExpired = processing(BASE_TIME, Duration.ofMinutes(10));
        MediaEncodeInbox newestExpired = processing(
                BASE_TIME.plus(Duration.ofMinutes(5)),
                Duration.ofMinutes(10)
        );
        MediaEncodeInbox exactBoundary = processing(
                BASE_TIME.plus(Duration.ofMinutes(10)),
                Duration.ofMinutes(50)
        );
        MediaEncodeInbox active = processing(
                BASE_TIME.plus(Duration.ofMinutes(20)),
                Duration.ofMinutes(50)
        );
        MediaEncodeInbox differentStatus = outputUploaded(
                BASE_TIME.plus(Duration.ofMinutes(30)),
                Duration.ofMinutes(10)
        );
        repository.saveAllAndFlush(List.of(
                active,
                newestExpired,
                differentStatus,
                exactBoundary,
                oldestExpired
        ));
        entityManager.clear();

        List<MediaEncodeInbox> result = repository
                .findTop100ByStatusAndLeaseUntilBeforeOrderByUpdatedAt(
                        InboxStatus.PROCESSING,
                        threshold
                );

        assertThat(result)
                .extracting(MediaEncodeInbox::getJobId)
                .containsExactly(oldestExpired.getJobId(), newestExpired.getJobId());
    }

    private MediaEncodeInbox processing(Instant updatedAt) {
        return processing(updatedAt, PROCESSING_LEASE);
    }

    private MediaEncodeInbox processing(Instant updatedAt, Duration lease) {
        UUID jobId = UUID.randomUUID();
        return MediaEncodeInbox.begin(
                jobId,
                jobId.toString(),
                "{\"jobId\":\"" + jobId + "\"}",
                updatedAt,
                lease
        );
    }

    private MediaEncodeInbox failurePending(Instant updatedAt) {
        MediaEncodeInbox inbox = processing(updatedAt.minusSeconds(1));
        inbox.recordFailure(
                FailureCode.ENCODE_FAILED,
                "repository integration failure",
                false,
                updatedAt
        );
        return inbox;
    }

    private MediaEncodeInbox outputUploaded(Instant updatedAt, Duration callbackLease) {
        MediaEncodeInbox inbox = processing(updatedAt.minusSeconds(1));
        inbox.outputUploaded(updatedAt, callbackLease);
        return inbox;
    }

    private MediaEncodeInbox done(Instant updatedAt) {
        MediaEncodeInbox inbox = outputUploaded(
                updatedAt.minusSeconds(1),
                Duration.ofMinutes(1)
        );
        inbox.done(updatedAt);
        return inbox;
    }
}
