package kr.co.onfilm.encodingworker.infra.inbox;

import kr.co.onfilm.encodingworker.application.FailureCode;
import kr.co.onfilm.encodingworker.domain.InboxStatus;
import kr.co.onfilm.encodingworker.domain.MediaEncodeInbox;
import kr.co.onfilm.encodingworker.support.MySqlContainerSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DataJpaTest(properties = "spring.sql.init.mode=never")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class MediaEncodeInboxConstraintIntegrationTest extends MySqlContainerSupport {

    private static final Instant NOW = Instant.parse("2026-09-02T02:00:00Z");
    private static final Duration LEASE = Duration.ofMinutes(10);

    @Autowired
    private MediaEncodeInboxRepository repository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidScalarUpdates")
    void scalarConstraintRejectsInvalidValue(InvalidUpdate invalid) {
        UUID jobId = insertValidProcessing();

        assertConstraintViolation(jobId, invalid);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidFailurePairUpdates")
    void failurePairConstraintRequiresCodeAndReasonTogether(InvalidUpdate invalid) {
        UUID jobId = insertValidProcessing();

        assertConstraintViolation(jobId, invalid);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidLeaseStatusUpdates")
    void leaseStatusConstraintRejectsInvalidStateCombination(InvalidUpdate invalid) {
        UUID jobId = insertValidProcessing();

        assertConstraintViolation(jobId, invalid);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidFailureStatusUpdates")
    void failureStatusConstraintRequiresFailureForRetryAndFailedStates(InvalidUpdate invalid) {
        UUID jobId = insertValidProcessing();

        assertConstraintViolation(jobId, invalid);
    }

    @Test
    void doneConstraintRejectsRemainingFailureInformation() {
        UUID jobId = insertValidProcessing();

        assertConstraintViolation(jobId, new InvalidUpdate(
                "done retains failure",
                "status = 'DONE', lease_until = NULL, "
                        + "failure_code = 'ENCODE_FAILED', failure_reason = 'old failure'",
                "ck_inbox_done_clears_failure"
        ));
    }

    @Test
    void allLegalInboxStatesAndCallbackRecoveryVariantCanBePersisted() {
        List<MediaEncodeInbox> legalStates = List.of(
                processing(),
                retryWait(),
                outputUploaded(),
                outputUploadedAfterCallbackFailure(),
                failurePending(),
                done(),
                failed()
        );

        repository.saveAllAndFlush(legalStates);

        assertThat(repository.findAllById(
                legalStates.stream().map(MediaEncodeInbox::getJobId).toList()
        )).hasSize(legalStates.size())
                .extracting(MediaEncodeInbox::getStatus)
                .containsExactlyInAnyOrder(
                        InboxStatus.PROCESSING,
                        InboxStatus.RETRY_WAIT,
                        InboxStatus.OUTPUT_UPLOADED,
                        InboxStatus.OUTPUT_UPLOADED,
                        InboxStatus.FAILURE_PENDING,
                        InboxStatus.DONE,
                        InboxStatus.FAILED
                );
    }

    private void assertConstraintViolation(UUID jobId, InvalidUpdate invalid) {
        assertThatThrownBy(() -> jdbcTemplate.update(
                "UPDATE media_encode_inbox SET " + invalid.setClause() + " WHERE job_id = ?",
                jobId.toString()
        )).isInstanceOf(DataAccessException.class)
                .hasMessageContaining(invalid.constraintName());
    }

    private UUID insertValidProcessing() {
        UUID jobId = UUID.randomUUID();
        jdbcTemplate.update("""
                        INSERT INTO media_encode_inbox (
                            job_id,
                            version,
                            status,
                            attempts,
                            lease_until,
                            created_at,
                            updated_at,
                            kafka_key,
                            payload,
                            failure_code,
                            failure_reason
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                jobId.toString(),
                0L,
                InboxStatus.PROCESSING.name(),
                1,
                Timestamp.from(NOW.plus(LEASE)),
                Timestamp.from(NOW),
                Timestamp.from(NOW),
                jobId.toString(),
                "{}",
                null,
                null
        );
        return jobId;
    }

    private MediaEncodeInbox processing() {
        return newInbox();
    }

    private MediaEncodeInbox retryWait() {
        MediaEncodeInbox inbox = newInbox();
        inbox.recordFailure(
                FailureCode.SOURCE_DOWNLOAD_FAILED,
                "retryable download failure",
                true,
                NOW.plusSeconds(1)
        );
        return inbox;
    }

    private MediaEncodeInbox outputUploaded() {
        MediaEncodeInbox inbox = newInbox();
        inbox.outputUploaded(NOW.plusSeconds(1), Duration.ofSeconds(30));
        return inbox;
    }

    private MediaEncodeInbox outputUploadedAfterCallbackFailure() {
        MediaEncodeInbox inbox = outputUploaded();
        inbox.recordFailure(
                FailureCode.CORE_API_UNAVAILABLE,
                "callback timeout",
                true,
                NOW.plusSeconds(2)
        );
        return inbox;
    }

    private MediaEncodeInbox failurePending() {
        MediaEncodeInbox inbox = newInbox();
        inbox.recordFailure(
                FailureCode.INVALID_REQUEST,
                "permanent request failure",
                false,
                NOW.plusSeconds(1)
        );
        return inbox;
    }

    private MediaEncodeInbox done() {
        MediaEncodeInbox inbox = outputUploaded();
        inbox.done(NOW.plusSeconds(2));
        return inbox;
    }

    private MediaEncodeInbox failed() {
        MediaEncodeInbox inbox = failurePending();
        inbox.failed(NOW.plusSeconds(2));
        return inbox;
    }

    private MediaEncodeInbox newInbox() {
        UUID jobId = UUID.randomUUID();
        return MediaEncodeInbox.begin(
                jobId,
                jobId.toString(),
                "{\"jobId\":\"" + jobId + "\"}",
                NOW,
                LEASE
        );
    }

    private static Stream<InvalidUpdate> invalidScalarUpdates() {
        return Stream.of(
                new InvalidUpdate(
                        "attempts must be positive",
                        "attempts = 0",
                        "ck_inbox_attempts_positive"
                ),
                new InvalidUpdate(
                        "version must not be negative",
                        "version = -1",
                        "ck_inbox_version_non_negative"
                ),
                new InvalidUpdate(
                        "updated time must not precede created time",
                        "updated_at = DATE_SUB(created_at, INTERVAL 1 SECOND)",
                        "ck_inbox_timestamp_order"
                ),
                new InvalidUpdate(
                        "lease must be later than updated time",
                        "lease_until = updated_at",
                        "ck_inbox_lease_after_update"
                ),
                new InvalidUpdate(
                        "payload must contain valid json",
                        "payload = 'not-json'",
                        "ck_inbox_payload_json"
                )
        );
    }

    private static Stream<InvalidUpdate> invalidFailurePairUpdates() {
        return Stream.of(
                new InvalidUpdate(
                        "failure code without reason",
                        "failure_code = 'ENCODE_FAILED', failure_reason = NULL",
                        "ck_inbox_failure_pair"
                ),
                new InvalidUpdate(
                        "failure reason without code",
                        "failure_code = NULL, failure_reason = 'reason only'",
                        "ck_inbox_failure_pair"
                ),
                new InvalidUpdate(
                        "blank failure reason",
                        "failure_code = 'ENCODE_FAILED', failure_reason = '   '",
                        "ck_inbox_failure_reason_not_blank"
                )
        );
    }

    private static Stream<InvalidUpdate> invalidLeaseStatusUpdates() {
        return Stream.of(
                new InvalidUpdate(
                        "processing without lease",
                        "lease_until = NULL",
                        "ck_inbox_lease_status"
                ),
                new InvalidUpdate(
                        "retry wait retains lease",
                        "status = 'RETRY_WAIT', failure_code = 'ENCODE_FAILED', "
                                + "failure_reason = 'retry failure'",
                        "ck_inbox_lease_status"
                ),
                new InvalidUpdate(
                        "failure pending retains lease",
                        "status = 'FAILURE_PENDING', failure_code = 'ENCODE_FAILED', "
                                + "failure_reason = 'pending failure'",
                        "ck_inbox_lease_status"
                ),
                new InvalidUpdate(
                        "done retains lease",
                        "status = 'DONE'",
                        "ck_inbox_lease_status"
                ),
                new InvalidUpdate(
                        "failed retains lease",
                        "status = 'FAILED', failure_code = 'ENCODE_FAILED', "
                                + "failure_reason = 'terminal failure'",
                        "ck_inbox_lease_status"
                )
        );
    }

    private static Stream<InvalidUpdate> invalidFailureStatusUpdates() {
        return Stream.of(
                new InvalidUpdate(
                        "retry wait without failure",
                        "status = 'RETRY_WAIT', lease_until = NULL",
                        "ck_inbox_failure_status"
                ),
                new InvalidUpdate(
                        "failure pending without failure",
                        "status = 'FAILURE_PENDING', lease_until = NULL",
                        "ck_inbox_failure_status"
                ),
                new InvalidUpdate(
                        "failed without failure",
                        "status = 'FAILED', lease_until = NULL",
                        "ck_inbox_failure_status"
                )
        );
    }

    private record InvalidUpdate(
            String description,
            String setClause,
            String constraintName
    ) {
        @Override
        public String toString() {
            return description;
        }
    }
}
