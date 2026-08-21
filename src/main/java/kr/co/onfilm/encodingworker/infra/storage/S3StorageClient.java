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
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

@Component
@ConditionalOnProperty(prefix = "app.storage", name = "type", havingValue = "s3")
@RequiredArgsConstructor
public class S3StorageClient implements StorageClient {

    private final S3Client s3Client;

    @Override
    public StorageObjectMetadata metadata(String bucket, String key) {
        try {
            HeadObjectResponse response = s3Client.headObject(HeadObjectRequest.builder()
                    .bucket(bucket).key(key).build());
            return new StorageObjectMetadata(response.contentLength(), response.contentType());
        } catch (S3Exception exception) {
            throw storageFailure("Failed to inspect s3://" + bucket + "/" + key, exception);
        }
    }

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
        } catch (S3Exception exception) {
            throw storageFailure("Failed to download s3://" + bucket + "/" + key, exception);
        } catch (RuntimeException exception) {
            throw new StorageException("Failed to download s3://" + bucket + "/" + key, true, exception);
        }
    }

    @Override
    public void uploadFiles(String bucket, String targetKey, List<Path> files, String contentType) {
        Path localBaseDir = localBaseDir(files);
        String targetBaseKey = targetBaseKey(targetKey);
        List<String> uploadedKeys = new java.util.ArrayList<>();
        try {
            for (Path file : StoragePaths.manifestLast(files)) {
                Path relative = localBaseDir.relativize(file);
                String objectKey = relative.toString().isBlank()
                        ? targetKey
                        : appendKey(targetBaseKey, relative);
                String resolvedContentType = ContentTypes.resolve(file, contentType);
                s3Client.putObject(PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(objectKey)
                            .contentType(resolvedContentType)
                            .contentLength(fileSize(file))
                            .build(),
                    RequestBody.fromFile(file));
                uploadedKeys.add(objectKey);
            }
        } catch (RuntimeException exception) {
            uploadedKeys.forEach(key -> deleteBestEffort(bucket, key, exception));
            if (exception instanceof S3Exception s3Exception) {
                throw storageFailure("Failed to upload encoded output", s3Exception);
            }
            throw new StorageException("Failed to upload encoded output", true, exception);
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

    private long fileSize(Path file) {
        try {
            return Files.size(file);
        } catch (IOException exception) {
            throw new StorageException("Failed to read output size: " + file, false, exception);
        }
    }

    private void deleteBestEffort(String bucket, String key, RuntimeException original) {
        try {
            s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
        } catch (RuntimeException cleanupFailure) {
            original.addSuppressed(cleanupFailure);
        }
    }

    private StorageException storageFailure(String message, S3Exception exception) {
        int status = exception.statusCode();
        boolean retryable = status == 0 || status == 408 || status == 429 || status >= 500;
        return new StorageException(message, retryable, exception);
    }
}
