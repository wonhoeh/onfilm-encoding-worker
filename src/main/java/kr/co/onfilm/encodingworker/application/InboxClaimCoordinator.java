package kr.co.onfilm.encodingworker.application;

import kr.co.onfilm.encodingworker.domain.InboxClaim;
import kr.co.onfilm.encodingworker.domain.MediaEncodeRequestedMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class InboxClaimCoordinator {
    private final InboxTransactionService transactions;

    public InboxClaim claim(String kafkaKey, MediaEncodeRequestedMessage message) {
        try {
            return transactions.claim(kafkaKey, message);
        } catch (DataIntegrityViolationException | CannotAcquireLockException race) {
            return transactions.claim(kafkaKey, message);
        }
    }
}
