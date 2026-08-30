package kr.co.onfilm.encodingworker.application;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import kr.co.onfilm.encodingworker.observability.CorrelationIdContext;
import kr.co.onfilm.encodingworker.observability.WorkerMediaEncodeMetrics;
import org.slf4j.MDC;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import static net.logstash.logback.argument.StructuredArguments.kv;

@Slf4j
@Service
@RequiredArgsConstructor
public class StaleInboxRecoveryService {
    private final InboxTransactionService transactions;
    private final EncodingJobProcessor processor;
    private final WorkerMediaEncodeMetrics metrics;

    @Scheduled(fixedDelayString = "${app.worker.stale-recovery-delay:60000}")
    public void recover() {
        transactions.staleProcessingJobs().forEach(snapshot -> {
            var message = snapshot.message();
            try (MDC.MDCCloseable ignoredCorrelation = MDC.putCloseable(
                         CorrelationIdContext.MDC_KEY,
                         CorrelationIdContext.resolve(message.correlationId(), message.requestId())
                 );
                 MDC.MDCCloseable ignoredJob = MDC.putCloseable("jobId", message.jobId().toString());
                 MDC.MDCCloseable ignoredRequest = MDC.putCloseable("requestId", message.requestId().toString())) {
                try {
                    metrics.recordStaleRecovery("started");
                    log.warn("Recovering stale inbox job. {} {}",
                            kv("eventType", "MEDIA_ENCODE_STALE_RECOVERY_STARTED"),
                            kv("status", "PROCESSING"));
                    processor.process(snapshot.kafkaKey(), message);
                    metrics.recordStaleRecovery("success");
                } catch (RuntimeException exception) {
                    metrics.recordStaleRecovery("failure");
                    log.warn("Stale inbox recovery attempt failed. {} {}",
                            kv("eventType", "MEDIA_ENCODE_STALE_RECOVERY_FAILED"),
                            kv("status", "PROCESSING"),
                            exception);
                }
            }
        });
    }
}
