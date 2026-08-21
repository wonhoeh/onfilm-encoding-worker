package kr.co.onfilm.encodingworker.domain;

public enum InboxStatus {
    PROCESSING,
    RETRY_WAIT,
    OUTPUT_UPLOADED,
    FAILURE_PENDING,
    DONE,
    FAILED
}
