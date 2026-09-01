CREATE TABLE media_encode_inbox (
    job_id VARCHAR(36) NOT NULL,
    version BIGINT NOT NULL,
    status ENUM (
        'DONE',
        'FAILED',
        'FAILURE_PENDING',
        'OUTPUT_UPLOADED',
        'PROCESSING',
        'RETRY_WAIT'
    ) NOT NULL,
    attempts INT NOT NULL,
    lease_until DATETIME(6) NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    kafka_key VARCHAR(36) NOT NULL,
    payload TEXT NOT NULL,
    failure_code ENUM (
        'CORE_API_UNAVAILABLE',
        'ENCODE_FAILED',
        'ENCODE_TIMEOUT',
        'INVALID_REQUEST',
        'OUTPUT_UPLOAD_FAILED',
        'OUTPUT_VALIDATION_FAILED',
        'SOURCE_DOWNLOAD_FAILED',
        'SOURCE_NOT_FOUND',
        'UNEXPECTED_WORKER_ERROR',
        'UNSUPPORTED_MEDIA',
        'UNSUPPORTED_MESSAGE_SCHEMA'
    ) NULL,
    failure_reason VARCHAR(1000) NULL,
    CONSTRAINT pk_media_encode_inbox PRIMARY KEY (job_id),
    INDEX idx_inbox_status_lease (status, lease_until),
    INDEX idx_inbox_failure_pending (status, updated_at)
) ENGINE = InnoDB;
