package kr.co.onfilm.encodingworker.infra.transcode;

public class TranscodeTimeoutException extends TranscodeException {
    public TranscodeTimeoutException(String message) {
        super(message);
    }
}
