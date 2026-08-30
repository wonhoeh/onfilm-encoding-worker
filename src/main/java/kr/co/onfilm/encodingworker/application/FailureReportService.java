package kr.co.onfilm.encodingworker.application;

import kr.co.onfilm.encodingworker.domain.InboxStatus;
import kr.co.onfilm.encodingworker.infra.coreapi.*;
import kr.co.onfilm.encodingworker.observability.CorrelationIdContext;
import kr.co.onfilm.encodingworker.observability.WorkerMediaEncodeMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.slf4j.MDC;

import java.time.Clock;
import java.util.UUID;

import static net.logstash.logback.argument.StructuredArguments.kv;

@Slf4j
@Service
@RequiredArgsConstructor
public class FailureReportService {
    private final InboxTransactionService transactions;
    private final CoreApiClient coreApiClient;
    private final Clock clock;
    private final WorkerMediaEncodeMetrics metrics;

    public void prepareAndReportIfEligible(UUID jobId) {
        InboxStatus status = transactions.failure(jobId)
                .map(InboxTransactionService.FailureSnapshot::status)
                .orElse(null);
        if (status == null || status == InboxStatus.PROCESSING
                || status == InboxStatus.OUTPUT_UPLOADED
                || status == InboxStatus.DONE || status == InboxStatus.FAILED) {
            return;
        }
        transactions.prepareFailureReport(jobId);
        report(jobId);
    }

    @Scheduled(fixedDelayString = "${app.worker.failure-report-delay:60000}")
    public void retryPendingReports() {
        transactions.pendingFailureReports().forEach(this::report);
    }

    private void report(UUID jobId) {
        InboxTransactionService.FailureSnapshot failure = transactions.failure(jobId)
                .orElseThrow(() -> new IllegalStateException("INBOX_JOB_NOT_FOUND"));
        if (failure.status() != InboxStatus.FAILURE_PENDING) return;
        long startedAt = System.nanoTime();
        String result = "error";
        try (MDC.MDCCloseable ignoredCorrelation = MDC.putCloseable(
                     CorrelationIdContext.MDC_KEY,
                     CorrelationIdContext.resolve(failure.correlationId(), failure.requestId())
             );
             MDC.MDCCloseable ignoredJob = MDC.putCloseable("jobId", jobId.toString());
             MDC.MDCCloseable ignoredRequest = MDC.putCloseable(
                     "requestId",
                     failure.requestId().toString()
             )) {
            try {
                coreApiClient.markFailed(jobId, failure.code(), failure.reason(), clock.instant());
                transactions.markFailed(jobId);
                result = "success";
                log.info("Media encode failure callback sent. {} {}",
                        kv("eventType", "MEDIA_ENCODE_FAILURE_CALLBACK_SENT"),
                        kv("status", "FAILED"));
            } catch (CoreApiException exception) {
                if (!exception.isRetryable()) {
                    log.error("Core API permanently rejected failure callback. {} {}",
                            kv("eventType", "MEDIA_ENCODE_FAILURE_CALLBACK_REJECTED"),
                            kv("retryable", false),
                            exception);
                    transactions.markFailed(jobId);
                    result = "permanent_failure";
                    return;
                }
                result = "retry";
                log.warn("Failure callback will be retried. {} {}",
                        kv("eventType", "MEDIA_ENCODE_FAILURE_CALLBACK_FAILED"),
                        kv("retryable", true),
                        exception);
            }
        } finally {
            metrics.recordCallback("failure", result, System.nanoTime() - startedAt);
        }
    }
}
