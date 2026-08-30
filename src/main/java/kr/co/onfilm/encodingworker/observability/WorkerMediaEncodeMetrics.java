package kr.co.onfilm.encodingworker.observability;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import kr.co.onfilm.encodingworker.application.FailureCode;
import kr.co.onfilm.encodingworker.domain.EncodeJobType;
import kr.co.onfilm.encodingworker.domain.InboxClaim;
import kr.co.onfilm.encodingworker.domain.InboxStatus;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class WorkerMediaEncodeMetrics {
    private final MeterRegistry registry;
    private final Map<InboxStatus, AtomicLong> inboxCounts = new EnumMap<>(InboxStatus.class);
    private final AtomicLong oldestFailurePendingAgeSeconds = new AtomicLong();

    public WorkerMediaEncodeMetrics(MeterRegistry registry) {
        this.registry = registry;
        registerInboxGauges();
    }

    public void recordInboxClaim(InboxClaim claim) {
        registry.counter(
                "media.encode.worker.inbox.claim",
                "result", lower(claim)
        ).increment();
    }

    public void recordStage(
            EncodeJobType jobType,
            String stage,
            String result,
            long elapsedNanos
    ) {
        Timer.builder("media.encode.worker.stage.duration")
                .tag("type", lower(jobType))
                .tag("stage", stage.toLowerCase(Locale.ROOT))
                .tag("result", result)
                .register(registry)
                .record(elapsedNanos, TimeUnit.NANOSECONDS);
    }

    public void recordAttempt(
            EncodeJobType jobType,
            String result,
            long elapsedNanos
    ) {
        registry.counter(
                "media.encode.worker.attempt",
                "type", lower(jobType),
                "result", result
        ).increment();
        Timer.builder("media.encode.worker.attempt.duration")
                .tag("type", lower(jobType))
                .tag("result", result)
                .register(registry)
                .record(elapsedNanos, TimeUnit.NANOSECONDS);
    }

    public void recordFailure(
            EncodeJobType jobType,
            String stage,
            FailureCode code,
            boolean retryable
    ) {
        registry.counter(
                "media.encode.worker.failure",
                "type", lower(jobType),
                "stage", stage.toLowerCase(Locale.ROOT),
                "code", lower(code),
                "retryable", Boolean.toString(retryable)
        ).increment();
    }

    public void recordCallback(String callbackType, String result, long elapsedNanos) {
        registry.counter(
                "media.encode.worker.callback",
                "type", callbackType,
                "result", result
        ).increment();
        Timer.builder("media.encode.worker.callback.duration")
                .tag("type", callbackType)
                .tag("result", result)
                .register(registry)
                .record(elapsedNanos, TimeUnit.NANOSECONDS);
    }

    public void recordStaleRecovery(String result) {
        registry.counter(
                "media.encode.worker.stale.recovery",
                "result", result
        ).increment();
    }

    public void recordDlt(String result) {
        registry.counter("media.encode.worker.dlt", "result", result).increment();
    }

    public void updateInboxCount(InboxStatus status, long count) {
        inboxCounts.get(status).set(Math.max(0, count));
    }

    public void updateOldestFailurePendingAge(Duration age) {
        oldestFailurePendingAgeSeconds.set(nonNegative(age).toSeconds());
    }

    private void registerInboxGauges() {
        for (InboxStatus status : InboxStatus.values()) {
            AtomicLong value = new AtomicLong();
            inboxCounts.put(status, value);
            Gauge.builder("media.encode.worker.inbox.records", value, AtomicLong::get)
                    .tag("status", lower(status))
                    .register(registry);
        }
        Gauge.builder(
                        "media.encode.worker.inbox.oldest.failure.pending.age",
                        oldestFailurePendingAgeSeconds,
                        AtomicLong::get
                )
                .baseUnit("seconds")
                .register(registry);
    }

    private Duration nonNegative(Duration duration) {
        if (duration == null || duration.isNegative()) {
            return Duration.ZERO;
        }
        return duration;
    }

    private String lower(Enum<?> value) {
        return value.name().toLowerCase(Locale.ROOT);
    }
}
