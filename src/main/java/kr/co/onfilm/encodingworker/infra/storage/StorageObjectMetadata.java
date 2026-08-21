package kr.co.onfilm.encodingworker.infra.storage;

public record StorageObjectMetadata(long contentLength, String contentType) {
    public StorageObjectMetadata {
        if (contentLength < 0) throw new IllegalArgumentException("contentLength must not be negative");
    }
}
