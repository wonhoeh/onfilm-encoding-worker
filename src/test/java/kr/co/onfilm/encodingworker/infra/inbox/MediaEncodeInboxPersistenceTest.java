package kr.co.onfilm.encodingworker.infra.inbox;

import kr.co.onfilm.encodingworker.domain.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.ActiveProfiles;

import java.time.*;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

@DataJpaTest
@ActiveProfiles("dev")
class MediaEncodeInboxPersistenceTest {
    @Autowired MediaEncodeInboxRepository repository;
    @Autowired TestEntityManager entityManager;

    @Test
    void jobIdIsUniqueAndVersioned() {
        UUID jobId = UUID.randomUUID();
        repository.saveAndFlush(MediaEncodeInbox.begin(
                jobId, jobId.toString(), "{}", Instant.now(), Duration.ofMinutes(10)));

        assertThat(repository.findById(jobId)).get()
                .extracting(MediaEncodeInbox::getVersion).isNotNull();
        entityManager.clear();
        assertThatThrownBy(() -> repository.saveAndFlush(MediaEncodeInbox.begin(
                jobId, jobId.toString(), "{}", Instant.now(), Duration.ofMinutes(10))))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void findsExpiredProcessingLeaseForCrashRecovery() {
        Instant now = Instant.parse("2026-08-21T00:00:00Z");
        UUID jobId = UUID.randomUUID();
        repository.saveAndFlush(MediaEncodeInbox.begin(
                jobId, jobId.toString(), "{}", now, Duration.ofMinutes(10)));

        assertThat(repository.findTop100ByStatusAndLeaseUntilBeforeOrderByUpdatedAt(
                InboxStatus.PROCESSING, now.plus(Duration.ofMinutes(11))))
                .extracting(MediaEncodeInbox::getJobId)
                .containsExactly(jobId);
    }
}
