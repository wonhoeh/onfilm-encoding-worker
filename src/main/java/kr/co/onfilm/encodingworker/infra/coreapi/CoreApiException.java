package kr.co.onfilm.encodingworker.infra.coreapi;

public class CoreApiException extends RuntimeException {

    public CoreApiException(String message, Throwable cause) {
        super(message, cause);
    }
}
