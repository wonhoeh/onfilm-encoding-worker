package kr.co.onfilm.encodingworker.infra.transcode;

public class TranscodeException extends RuntimeException {

    public TranscodeException(String message, Throwable cause) {
        super(message, cause);
    }

    public TranscodeException(String message) {
        super(message);
    }
}
