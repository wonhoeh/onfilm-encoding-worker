package kr.co.onfilm.encodingworker.infra.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Component
@ConditionalOnProperty(prefix = "app.storage", name = "type", havingValue = "s3")
@RequiredArgsConstructor
public class S3StorageClient implements StorageClient {

    private final S3Client s3Client;

    @Override
    public Path download(String bucket, String key, Path destination) {
        try {
            Files.createDirectories(destination.getParent());
            s3Client.getObject(GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build(), destination);
            return destination;
        } catch (IOException exception) {
            throw new StorageException("Failed to prepare download path for " + key, exception);
        } catch (RuntimeException exception) {
            throw new StorageException("Failed to download s3://" + bucket + "/" + key, exception);
        }
    }

    @Override
    public void uploadFiles(String bucket, String targetKey, List<Path> files, String contentType) {
        Path localBaseDir = localBaseDir(files);
        String targetBaseKey = targetBaseKey(targetKey);
        for (Path file : files) {
            Path relative = localBaseDir.relativize(file);
            String objectKey = relative.toString().isBlank()
                    ? targetKey
                    : appendKey(targetBaseKey, relative);
            String resolvedContentType = ContentTypes.resolve(file, contentType);
            s3Client.putObject(PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(objectKey)
                            .contentType(resolvedContentType)
                            .build(),
                    RequestBody.fromFile(file));
        }
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
