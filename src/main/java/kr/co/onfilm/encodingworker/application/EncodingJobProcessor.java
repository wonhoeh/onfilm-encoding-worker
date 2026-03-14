package kr.co.onfilm.encodingworker.application;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import kr.co.onfilm.encodingworker.config.AppProperties;
import kr.co.onfilm.encodingworker.domain.EncodedOutput;
import kr.co.onfilm.encodingworker.domain.MediaEncodeRequestedMessage;
import kr.co.onfilm.encodingworker.infra.coreapi.CoreApiClient;
import kr.co.onfilm.encodingworker.infra.storage.S3StorageClient;
import kr.co.onfilm.encodingworker.infra.transcode.FfmpegTranscoder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class EncodingJobProcessor {

    private final AppProperties properties;
    private final EncodeRequestValidator validator;
    private final CoreApiClient coreApiClient;
    private final S3StorageClient s3StorageClient;
    private final FfmpegTranscoder transcoder;

    public void process(MediaEncodeRequestedMessage message) {
        Instant startedAt = Instant.now();
        Path jobDir = Path.of(properties.worker().workingDir()).resolve(message.jobId().toString());
        try {
            validator.validate(message);
            coreApiClient.markProcessing(message.jobId(), startedAt);

            Path sourceFile = s3StorageClient.download(
                    message.sourceBucket(),
                    message.sourceKey(),
                    jobDir.resolve("source").resolve(sourceFileName(message.sourceKey()))
            );
            EncodedOutput output = transcoder.transcode(message, sourceFile, jobDir.resolve("output"));
            s3StorageClient.uploadFiles(message.targetBucket(), message.targetKey(), output.files(), output.contentType());
            coreApiClient.updateMediaPath(message.jobType(), message.movieId(), message.jobId(), message.targetKey());
            coreApiClient.markDone(message.jobId(), Instant.now());
        } catch (Exception exception) {
            log.error("Encoding job failed. jobId={}", message.jobId(), exception);
            coreApiClient.markFailed(message.jobId(), summarizeFailure(exception), Instant.now());
            throw exception;
        } finally {
            cleanup(jobDir);
        }
    }

    private String sourceFileName(String sourceKey) {
        return Path.of(sourceKey).getFileName().toString();
    }

    private String summarizeFailure(Exception exception) {
        String message = ExceptionUtils.getRootCauseMessage(exception);
        String resolved = message == null ? exception.getMessage() : message;
        return resolved == null ? exception.getClass().getSimpleName() : resolved;
    }

    private void cleanup(Path jobDir) {
        if (Files.notExists(jobDir)) {
            return;
        }
        try (var walk = Files.walk(jobDir)) {
            walk.sorted((left, right) -> right.getNameCount() - left.getNameCount())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException ignored) {
                            log.warn("Failed to delete temp path {}", path);
                        }
                    });
        } catch (IOException ignored) {
            log.warn("Failed to cleanup working directory {}", jobDir);
        }
    }
}
