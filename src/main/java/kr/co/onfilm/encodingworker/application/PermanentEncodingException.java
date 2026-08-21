package kr.co.onfilm.encodingworker.application;

public class PermanentEncodingException extends RuntimeException {
    private final FailureCode failureCode;

    public PermanentEncodingException(FailureCode failureCode, String message) {
        super(message);
        this.failureCode = failureCode;
    }

    public PermanentEncodingException(FailureCode failureCode, String message, Throwable cause) {
        super(message, cause);
        this.failureCode = failureCode;
    }

    public FailureCode getFailureCode() {
        return failureCode;
    }
}
