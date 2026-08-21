package kr.co.onfilm.encodingworker.domain;

import kr.co.onfilm.encodingworker.application.FailureCode;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

class MediaEncodeInboxTest {
    private static final Instant NOW = Instant.parse("2026-08-21T00:00:00Z");
    private static final Duration LEASE = Duration.ofMinutes(10);

    @Test
    void activeLeaseRejectsDuplicateAndExpiredLeaseCanBeReclaimed() {
        MediaEncodeInbox inbox = inbox();
        assertThat(inbox.claim(NOW.plusSeconds(1), LEASE)).isEqualTo(InboxClaim.BUSY);
        assertThat(inbox.claim(NOW.plus(LEASE).plusSeconds(1), LEASE)).isEqualTo(InboxClaim.PROCESS);
        assertThat(inbox.getAttempts()).isEqualTo(2);
    }

    @Test
    void uploadedOutputRetriesOnlyCallbackAndDoneIsTerminal() {
        MediaEncodeInbox inbox = inbox();
        inbox.outputUploaded(NOW.plusSeconds(1), Duration.ofSeconds(30));
        inbox.recordFailure(FailureCode.CORE_API_UNAVAILABLE, "timeout", true, NOW.plusSeconds(2));

        assertThat(inbox.getStatus()).isEqualTo(InboxStatus.OUTPUT_UPLOADED);
        assertThat(inbox.claim(NOW.plusSeconds(3), LEASE)).isEqualTo(InboxClaim.CALLBACK_ONLY);
        inbox.done(NOW.plusSeconds(4));
        assertThat(inbox.claim(NOW.plusSeconds(5), LEASE)).isEqualTo(InboxClaim.TERMINAL);
    }

    @Test
    void transientFailureCanRetryButPermanentFailureWaitsForReport() {
        MediaEncodeInbox transientInbox = inbox();
        transientInbox.recordFailure(
                FailureCode.SOURCE_DOWNLOAD_FAILED, "network", true, NOW.plusSeconds(1));
        assertThat(transientInbox.getStatus()).isEqualTo(InboxStatus.RETRY_WAIT);
        assertThat(transientInbox.claim(NOW.plusSeconds(2), LEASE)).isEqualTo(InboxClaim.PROCESS);

        MediaEncodeInbox permanentInbox = inbox();
        permanentInbox.recordFailure(
                FailureCode.INVALID_REQUEST, "bad key", false, NOW.plusSeconds(1));
        assertThat(permanentInbox.getStatus()).isEqualTo(InboxStatus.FAILURE_PENDING);
        assertThat(permanentInbox.claim(NOW.plusSeconds(2), LEASE)).isEqualTo(InboxClaim.TERMINAL);
    }

    @Test
    void permanentCompletionCallbackFailureBecomesFailurePending() {
        MediaEncodeInbox inbox = inbox();
        inbox.outputUploaded(NOW.plusSeconds(1), Duration.ofSeconds(30));

        inbox.recordFailure(
                FailureCode.INVALID_REQUEST, "callback rejected", false, NOW.plusSeconds(2));

        assertThat(inbox.getStatus()).isEqualTo(InboxStatus.FAILURE_PENDING);
        assertThat(inbox.claim(NOW.plusSeconds(3), LEASE)).isEqualTo(InboxClaim.TERMINAL);
    }

    private MediaEncodeInbox inbox() {
        UUID jobId = UUID.randomUUID();
        return MediaEncodeInbox.begin(jobId, jobId.toString(), "{}", NOW, LEASE);
    }
}
