package kr.co.onfilm.encodingworker.application;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class StaleInboxRecoveryService {
    private final InboxTransactionService transactions;
    private final EncodingJobProcessor processor;

    @Scheduled(fixedDelayString = "${app.worker.stale-recovery-delay:60000}")
    public void recover() {
        transactions.staleProcessingJobs().forEach(snapshot -> {
            try {
                log.warn("Recovering stale inbox job. jobId={}", snapshot.message().jobId());
                processor.process(snapshot.kafkaKey(), snapshot.message());
            } catch (RuntimeException exception) {
                log.warn("Stale inbox recovery attempt failed. jobId={}",
                        snapshot.message().jobId(), exception);
            }
        });
    }
}
