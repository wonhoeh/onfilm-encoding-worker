package kr.co.onfilm.encodingworker.application;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import kr.co.onfilm.encodingworker.domain.InboxStatus;
import kr.co.onfilm.encodingworker.infra.coreapi.CoreApiClient;
import kr.co.onfilm.encodingworker.infra.coreapi.CoreApiException;
import kr.co.onfilm.encodingworker.observability.WorkerMediaEncodeMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FailureReportServiceTest {
    private static final Instant NOW = Instant.parse("2026-08-30T00:00:00Z");

    @Mock InboxTransactionService transactions;
    @Mock CoreApiClient coreApiClient;

    private SimpleMeterRegistry registry;
    private FailureReportService service;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        service = new FailureReportService(
                transactions,
                coreApiClient,
                Clock.fixed(NOW, ZoneOffset.UTC),
                new WorkerMediaEncodeMetrics(registry)
        );
    }

    @Test
    void recordsSuccessfulFailureCallback() {
        UUID jobId = UUID.randomUUID();
        UUID requestId = UUID.randomUUID();
        given(transactions.failure(jobId)).willReturn(
                Optional.of(snapshot(InboxStatus.RETRY_WAIT, requestId)),
                Optional.of(snapshot(InboxStatus.FAILURE_PENDING, requestId))
        );

        service.prepareAndReportIfEligible(jobId);

        verify(transactions).prepareFailureReport(jobId);
        verify(coreApiClient).markFailed(
                jobId, FailureCode.ENCODE_FAILED, "encode failed", NOW);
        verify(transactions).markFailed(jobId);
        assertThat(registry.counter(
                "media.encode.worker.callback", "type", "failure", "result", "success"
        ).count()).isEqualTo(1);
    }

    @Test
    void recordsRetryableFailureCallbackWithoutClosingInbox() {
        UUID jobId = UUID.randomUUID();
        UUID requestId = UUID.randomUUID();
        given(transactions.failure(jobId)).willReturn(
                Optional.of(snapshot(InboxStatus.RETRY_WAIT, requestId)),
                Optional.of(snapshot(InboxStatus.FAILURE_PENDING, requestId))
        );
        doThrow(new CoreApiException("timeout", true, null)).when(coreApiClient)
                .markFailed(jobId, FailureCode.ENCODE_FAILED, "encode failed", NOW);

        service.prepareAndReportIfEligible(jobId);

        verify(transactions, never()).markFailed(jobId);
        assertThat(registry.counter(
                "media.encode.worker.callback", "type", "failure", "result", "retry"
        ).count()).isEqualTo(1);
    }

    private InboxTransactionService.FailureSnapshot snapshot(
            InboxStatus status,
            UUID requestId
    ) {
        return new InboxTransactionService.FailureSnapshot(
                FailureCode.ENCODE_FAILED,
                "encode failed",
                status,
                "corr-123",
                requestId
        );
    }
}
