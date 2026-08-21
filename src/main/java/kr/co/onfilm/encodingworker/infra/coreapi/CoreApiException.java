package kr.co.onfilm.encodingworker.infra.coreapi;

public class CoreApiException extends RuntimeException {
    private final boolean retryable;

    public CoreApiException(String message, boolean retryable, Throwable cause) {
        super(message, cause);
        this.retryable = retryable;
    }

    public boolean isRetryable() {
        return retryable;
    }
}
