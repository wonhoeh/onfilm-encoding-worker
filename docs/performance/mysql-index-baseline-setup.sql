-- MySQL 8.4 / OnFilm Worker V1~V2 index benchmark dataset
-- This script is destructive only to the dedicated benchmark database/container.

USE onfilm_worker;

DELETE FROM media_encode_inbox;
DROP TABLE IF EXISTS benchmark_sequence;

CREATE TABLE benchmark_sequence (
    n INT NOT NULL,
    CONSTRAINT pk_benchmark_sequence PRIMARY KEY (n)
) ENGINE = InnoDB;

INSERT INTO benchmark_sequence (n)
SELECT d5.n * 100000 + d4.n * 10000 + d3.n * 1000
     + d2.n * 100 + d1.n * 10 + d0.n
FROM (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
      UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d0
CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
            UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d1
CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
            UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d2
CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
            UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d3
CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
            UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d4
CROSS JOIN (SELECT 0 n UNION ALL SELECT 1) d5
WHERE d5.n * 100000 + d4.n * 10000 + d3.n * 1000
    + d2.n * 100 + d1.n * 10 + d0.n < 200000;

INSERT INTO media_encode_inbox (
    job_id,
    version,
    status,
    attempts,
    lease_until,
    created_at,
    updated_at,
    kafka_key,
    payload,
    failure_code,
    failure_reason
)
SELECT CONCAT('30000000-0000-0000-0000-', LPAD(n + 1, 12, '0')),
       0,
       CASE
           WHEN MOD(n, 20) < 12 THEN 'DONE'
           WHEN MOD(n, 20) < 14 THEN 'FAILED'
           WHEN MOD(n, 20) < 16 THEN 'PROCESSING'
           WHEN MOD(n, 20) = 16 THEN 'OUTPUT_UPLOADED'
           WHEN MOD(n, 20) = 17 THEN 'FAILURE_PENDING'
           ELSE 'RETRY_WAIT'
       END,
       MOD(n, 5) + 1,
       CASE
           WHEN MOD(n, 20) IN (14, 15)
               THEN CASE
                   WHEN MOD(FLOOR(n / 20), 100) = 0
                       THEN TIMESTAMP'2026-05-31 12:00:00'
                   ELSE TIMESTAMP'2026-06-02 12:00:00'
               END
           WHEN MOD(n, 40) = 16
               THEN CASE
                   WHEN MOD(FLOOR(n / 40), 50) = 0
                       THEN TIMESTAMP'2026-05-31 12:00:00'
                   ELSE TIMESTAMP'2026-06-02 12:00:00'
               END
           ELSE NULL
       END,
       TIMESTAMPADD(MINUTE, n, '2026-01-01 00:00:00'),
       TIMESTAMPADD(MINUTE, n + 3, '2026-01-01 00:00:00'),
       CONCAT('30000000-0000-0000-0000-', LPAD(n + 1, 12, '0')),
       CONCAT(
           '{"schemaVersion":1,"jobId":"30000000-0000-0000-0000-',
           LPAD(n + 1, 12, '0'),
           '"}'
       ),
       CASE
           WHEN MOD(n, 20) IN (12, 13, 17, 18, 19)
                OR MOD(n, 40) = 36
               THEN 'ENCODE_FAILED'
           ELSE NULL
       END,
       CASE
           WHEN MOD(n, 20) IN (12, 13, 17, 18, 19)
                OR MOD(n, 40) = 36
               THEN 'benchmark worker failure'
           ELSE NULL
       END
FROM benchmark_sequence;

ANALYZE TABLE media_encode_inbox;

SELECT status, COUNT(*) row_count
FROM media_encode_inbox
GROUP BY status
ORDER BY status;

SELECT status,
       SUM(lease_until < '2026-06-01 00:00:00') stale_count,
       SUM(lease_until >= '2026-06-01 00:00:00') active_count,
       SUM(lease_until IS NULL) no_lease_count
FROM media_encode_inbox
GROUP BY status
ORDER BY status;
