USE onfilm_worker;

-- Q1. MediaEncodeInboxRepository.findByJobIdForUpdate(...)
-- FOR UPDATE is excluded because this measurement compares access paths.
EXPLAIN ANALYZE
SELECT i.*
FROM media_encode_inbox i
WHERE i.job_id = '30000000-0000-0000-0000-000000100001';

-- Q2. MediaEncodeInboxRepository.findTop100ByStatusOrderByUpdatedAt(FAILURE_PENDING)
EXPLAIN ANALYZE
SELECT i.*
FROM media_encode_inbox i
WHERE i.status = 'FAILURE_PENDING'
ORDER BY i.updated_at
LIMIT 100;

-- Q3. stale PROCESSING branch of staleProcessingJobs()
EXPLAIN ANALYZE
SELECT i.*
FROM media_encode_inbox i
WHERE i.status = 'PROCESSING'
  AND i.lease_until < '2026-06-01 00:00:00'
ORDER BY i.updated_at
LIMIT 100;

-- Q4. stale OUTPUT_UPLOADED branch of staleProcessingJobs()
EXPLAIN ANALYZE
SELECT i.*
FROM media_encode_inbox i
WHERE i.status = 'OUTPUT_UPLOADED'
  AND i.lease_until < '2026-06-01 00:00:00'
ORDER BY i.updated_at
LIMIT 100;

-- Q5. MediaEncodeInboxRepository.countByStatus(PROCESSING)
EXPLAIN ANALYZE
SELECT COUNT(*)
FROM media_encode_inbox i
WHERE i.status = 'PROCESSING';

-- Q6. MediaEncodeInboxRepository.findOldestUpdatedAtByStatus(FAILURE_PENDING)
EXPLAIN ANALYZE
SELECT MIN(i.updated_at)
FROM media_encode_inbox i
WHERE i.status = 'FAILURE_PENDING';

SELECT index_name, seq_in_index, column_name, cardinality, is_visible
FROM information_schema.statistics
WHERE table_schema = DATABASE()
  AND table_name = 'media_encode_inbox'
ORDER BY index_name, seq_in_index;
