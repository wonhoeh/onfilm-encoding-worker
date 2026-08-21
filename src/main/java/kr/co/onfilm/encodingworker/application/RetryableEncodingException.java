package kr.co.onfilm.encodingworker.application;

public class RetryableEncodingException extends RuntimeException {
    private final FailureCode failureCode;

    public RetryableEncodingException(FailureCode failureCode, String message, Throwable cause) {
        super(message, cause);
        this.failureCode = failureCode;
    }

    public FailureCode getFailureCode() {
        return failureCode;
    }
}
