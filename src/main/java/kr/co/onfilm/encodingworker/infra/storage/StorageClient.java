package kr.co.onfilm.encodingworker.infra.storage;

import java.nio.file.Path;
import java.util.List;

public interface StorageClient {

    Path download(String bucket, String key, Path destination);

    void uploadFiles(String bucket, String targetKey, List<Path> files, String contentType);
}
