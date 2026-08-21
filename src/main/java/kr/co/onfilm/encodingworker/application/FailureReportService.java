package kr.co.onfilm.encodingworker.application;

import kr.co.onfilm.encodingworker.domain.InboxStatus;
import kr.co.onfilm.encodingworker.infra.coreapi.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class FailureReportService {
    private final InboxTransactionService transactions;
    private final CoreApiClient coreApiClient;
    private final Clock clock;

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
        try {
            coreApiClient.markFailed(jobId, failure.code(), failure.reason(), clock.instant());
            transactions.markFailed(jobId);
        } catch (CoreApiException exception) {
            if (!exception.isRetryable()) {
                log.error("Core API permanently rejected failure callback. jobId={}", jobId, exception);
                transactions.markFailed(jobId);
                return;
            }
            log.warn("Failure callback will be retried. jobId={}", jobId, exception);
        }
    }
}
