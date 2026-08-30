package kr.co.onfilm.encodingworker.infra.inbox;

import jakarta.persistence.LockModeType;
import kr.co.onfilm.encodingworker.domain.*;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;

import java.util.*;
import java.time.Instant;

public interface MediaEncodeInboxRepository extends JpaRepository<MediaEncodeInbox, UUID> {
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select i from MediaEncodeInbox i where i.jobId = :jobId")
    Optional<MediaEncodeInbox> findByJobIdForUpdate(@Param("jobId") UUID jobId);

    long countByStatus(InboxStatus status);

    @Query("select min(i.updatedAt) from MediaEncodeInbox i where i.status = :status")
    Optional<Instant> findOldestUpdatedAtByStatus(@Param("status") InboxStatus status);

    List<MediaEncodeInbox> findTop100ByStatusOrderByUpdatedAt(InboxStatus status);

    List<MediaEncodeInbox> findTop100ByStatusAndLeaseUntilBeforeOrderByUpdatedAt(
            InboxStatus status, Instant leaseUntil);
}
