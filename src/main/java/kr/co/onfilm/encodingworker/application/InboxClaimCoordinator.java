package kr.co.onfilm.encodingworker.application;

import kr.co.onfilm.encodingworker.domain.InboxClaim;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import java.util.UUID;
import kr.co.onfilm.encodingworker.domain.MediaEncodeRequestedMessage;

@Service
@RequiredArgsConstructor
public class InboxClaimCoordinator {
    private final InboxTransactionService transactions;

    public InboxClaim claim(String kafkaKey, MediaEncodeRequestedMessage message) {
        try {
            return transactions.claim(kafkaKey, message);
        } catch (DataIntegrityViolationException race) {
            return transactions.claim(kafkaKey, message);
        }
    }
}
