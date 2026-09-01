ALTER TABLE media_encode_inbox
    ADD CONSTRAINT ck_inbox_attempts_positive
        CHECK (attempts >= 1),
    ADD CONSTRAINT ck_inbox_version_non_negative
        CHECK (version >= 0),
    ADD CONSTRAINT ck_inbox_timestamp_order
        CHECK (updated_at >= created_at),
    ADD CONSTRAINT ck_inbox_lease_after_update
        CHECK (lease_until IS NULL OR lease_until > updated_at),
    ADD CONSTRAINT ck_inbox_failure_pair
        CHECK (
            (failure_code IS NULL AND failure_reason IS NULL)
            OR (failure_code IS NOT NULL AND failure_reason IS NOT NULL)
        ),
    ADD CONSTRAINT ck_inbox_failure_reason_not_blank
        CHECK (failure_reason IS NULL OR CHAR_LENGTH(TRIM(failure_reason)) > 0),
    ADD CONSTRAINT ck_inbox_lease_status
        CHECK (
            (status = 'PROCESSING' AND lease_until IS NOT NULL)
            OR status = 'OUTPUT_UPLOADED'
            OR (
                status IN ('RETRY_WAIT', 'FAILURE_PENDING', 'DONE', 'FAILED')
                AND lease_until IS NULL
            )
        ),
    ADD CONSTRAINT ck_inbox_failure_status
        CHECK (
            status NOT IN ('RETRY_WAIT', 'FAILURE_PENDING', 'FAILED')
            OR (failure_code IS NOT NULL AND failure_reason IS NOT NULL)
        ),
    ADD CONSTRAINT ck_inbox_done_clears_failure
        CHECK (
            status <> 'DONE'
            OR (failure_code IS NULL AND failure_reason IS NULL)
        ),
    ADD CONSTRAINT ck_inbox_payload_json
        CHECK (JSON_VALID(payload));
