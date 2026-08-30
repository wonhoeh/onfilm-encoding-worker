package kr.co.onfilm.encodingworker.domain;

import jakarta.persistence.*;
import kr.co.onfilm.encodingworker.application.FailureCode;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "media_encode_inbox",
        indexes = {
                @Index(name = "idx_inbox_status_lease", columnList = "status,lease_until"),
                @Index(name = "idx_inbox_failure_pending", columnList = "status,updated_at")
        })
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class MediaEncodeInbox {
    @Id
    @Column(name = "job_id", nullable = false, updatable = false, length = 36)
    private UUID jobId;

    @Version
    @Column(nullable = false)
    private Long version;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private InboxStatus status;

    @Column(nullable = false)
    private int attempts;

    @Column(name = "lease_until")
    private Instant leaseUntil;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Column(name = "kafka_key", nullable = false, updatable = false, length = 36)
    private String kafkaKey;

    @Lob
    @Column(nullable = false, updatable = false)
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "failure_code", length = 64)
    private FailureCode failureCode;

    @Column(name = "failure_reason", length = 1000)
    private String failureReason;

    private MediaEncodeInbox(UUID jobId, String kafkaKey, String payload, Instant now, Duration lease) {
        this.jobId = require(jobId, "jobId");
        this.kafkaKey = require(kafkaKey, "kafkaKey");
        this.payload = require(payload, "payload");
        this.status = InboxStatus.PROCESSING;
        this.attempts = 1;
        this.leaseUntil = require(now, "now").plus(requirePositive(lease));
        this.createdAt = now;
        this.updatedAt = now;
    }

    public static MediaEncodeInbox begin(UUID jobId, String kafkaKey, String payload,
                                         Instant now, Duration lease) {
        return new MediaEncodeInbox(jobId, kafkaKey, payload, now, lease);
    }

    public boolean hasSameRequest(String kafkaKey, String payload) {
        return this.kafkaKey.equals(require(kafkaKey, "kafkaKey"))
                && this.payload.equals(require(payload, "payload"));
    }

    public InboxClaim claim(Instant now, Duration lease) {
        require(now, "now");
        requirePositive(lease);
        if (status == InboxStatus.DONE || status == InboxStatus.FAILED || status == InboxStatus.FAILURE_PENDING) {
            return InboxClaim.TERMINAL;
        }
        if (status == InboxStatus.OUTPUT_UPLOADED) {
            if (leaseUntil != null && leaseUntil.isAfter(now)) {
                return InboxClaim.BUSY;
            }
            leaseUntil = now.plus(lease);
            updatedAt = now;
            attempts++;
            return InboxClaim.CALLBACK_ONLY;
        }
        if (status == InboxStatus.PROCESSING && leaseUntil != null && leaseUntil.isAfter(now)) {
            return InboxClaim.BUSY;
        }
        status = InboxStatus.PROCESSING;
        leaseUntil = now.plus(lease);
        updatedAt = now;
        attempts++;
        return InboxClaim.PROCESS;
    }

    public void outputUploaded(Instant now, Duration callbackLease) {
        requireStatus(InboxStatus.PROCESSING);
        status = InboxStatus.OUTPUT_UPLOADED;
        leaseUntil = require(now, "now").plus(requirePositive(callbackLease));
        updatedAt = now;
    }

    public void done(Instant now) {
        if (status == InboxStatus.DONE) return;
        if (status != InboxStatus.OUTPUT_UPLOADED) throw new IllegalStateException("INBOX_OUTPUT_NOT_UPLOADED");
        status = InboxStatus.DONE;
        leaseUntil = null;
        updatedAt = require(now, "now");
        failureCode = null;
        failureReason = null;
    }

    public void recordFailure(FailureCode code, String reason, boolean retryable, Instant now) {
        if (status == InboxStatus.DONE || status == InboxStatus.FAILED) return;
        this.failureCode = require(code, "failureCode");
        this.failureReason = normalizeReason(reason);
        this.updatedAt = require(now, "now");
        this.leaseUntil = null;
        if (status == InboxStatus.OUTPUT_UPLOADED) {
            if (!retryable) status = InboxStatus.FAILURE_PENDING;
            return;
        }
        status = retryable ? InboxStatus.RETRY_WAIT : InboxStatus.FAILURE_PENDING;
    }

    public void prepareFailureReport(Instant now) {
        if (status == InboxStatus.DONE || status == InboxStatus.FAILED) return;
        status = InboxStatus.FAILURE_PENDING;
        leaseUntil = null;
        updatedAt = require(now, "now");
        if (failureCode == null) failureCode = FailureCode.UNEXPECTED_WORKER_ERROR;
        if (failureReason == null) failureReason = "Encoding retries exhausted";
    }

    public void failed(Instant now) {
        if (status == InboxStatus.FAILED) return;
        if (status != InboxStatus.FAILURE_PENDING) throw new IllegalStateException("INBOX_FAILURE_NOT_PENDING");
        status = InboxStatus.FAILED;
        updatedAt = require(now, "now");
    }

    private void requireStatus(InboxStatus expected) {
        if (status != expected) throw new IllegalStateException("INVALID_INBOX_STATUS");
    }

    private static String normalizeReason(String reason) {
        String value = reason == null || reason.isBlank() ? "Unknown worker failure" : reason.trim();
        return value.length() <= 1000 ? value : value.substring(0, 1000);
    }

    private static Duration requirePositive(Duration duration) {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("lease must be positive");
        }
        return duration;
    }

    private static <T> T require(T value, String field) {
        if (value == null) throw new IllegalArgumentException(field + " is required");
        return value;
    }
}
