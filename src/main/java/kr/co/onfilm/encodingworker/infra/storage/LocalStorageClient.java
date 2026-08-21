package kr.co.onfilm.encodingworker.infra.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import kr.co.onfilm.encodingworker.config.AppProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "app.storage", name = "type", havingValue = "local")
@RequiredArgsConstructor
public class LocalStorageClient implements StorageClient {

    private final AppProperties properties;

    @Override
    public StorageObjectMetadata metadata(String bucket, String key) {
        Path path = resolvePath(bucket, key);
        try {
            if (!Files.isRegularFile(path)) {
                throw new StorageException("Source object does not exist: " + key, false, null);
            }
            return new StorageObjectMetadata(Files.size(path), Files.probeContentType(path));
        } catch (IOException exception) {
            throw new StorageException("Failed to inspect local source " + path, true, exception);
        }
    }

    @Override
    public Path download(String bucket, String key, Path destination) {
        Path source = resolvePath(bucket, key);
        try {
            Files.createDirectories(destination.getParent());
            Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING);
            return destination;
        } catch (IOException exception) {
            throw new StorageException("Failed to copy local source " + source, exception);
        }
    }

    @Override
    public void uploadFiles(String bucket, String targetKey, List<Path> files, String contentType) {
        Path localBaseDir = localBaseDir(files);
        String targetBaseKey = targetBaseKey(targetKey);
        List<Path> uploaded = new java.util.ArrayList<>();
        try {
            for (Path file : StoragePaths.manifestLast(files)) {
                Path relative = localBaseDir.relativize(file);
                String objectKey = relative.toString().isBlank()
                        ? targetKey
                        : appendKey(targetBaseKey, relative);
                Path destination = resolvePath(bucket, objectKey);
                Files.createDirectories(destination.getParent());
                Files.copy(file, destination, StandardCopyOption.REPLACE_EXISTING);
                uploaded.add(destination);
            }
        } catch (IOException exception) {
            uploaded.forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException cleanupFailure) {
                    exception.addSuppressed(cleanupFailure);
                }
            });
            throw new StorageException("Failed to upload local encoded output", true, exception);
        }
    }

    private Path resolvePath(String bucket, String key) {
        String localRoot = properties.storage().localRoot();
        if (localRoot == null || localRoot.isBlank()) {
            throw new StorageException("app.storage.local-root must be configured when app.storage.type=local");
        }
        return StoragePaths.resolveBelow(Path.of(localRoot), bucket, key);
    }

    private Path localBaseDir(List<Path> files) {
        if (files.isEmpty()) {
            throw new StorageException("No output files to upload");
        }
        return files.get(0).getParent();
    }

    private String targetBaseKey(String targetKey) {
        Path parent = Path.of(targetKey).getParent();
        return parent == null ? "" : parent.toString().replace('\\', '/');
    }

    private String appendKey(String baseKey, Path relativeFile) {
        String relative = relativeFile.toString().replace('\\', '/');
        return baseKey.isBlank() ? relative : baseKey + "/" + relative;
    }
}
