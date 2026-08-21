package kr.co.onfilm.encodingworker.infra.storage;

public class StorageException extends RuntimeException {
    private final boolean retryable;

    public StorageException(String message) {
        this(message, false, null);
    }

    public StorageException(String message, Throwable cause) {
        this(message, true, cause);
    }

    public StorageException(String message, boolean retryable, Throwable cause) {
        super(message, cause);
        this.retryable = retryable;
    }

    public boolean isRetryable() {
        return retryable;
    }
}
