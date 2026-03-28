package kr.co.onfilm.encodingworker.application;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import kr.co.onfilm.encodingworker.config.AppProperties;
import kr.co.onfilm.encodingworker.domain.EncodedOutput;
import kr.co.onfilm.encodingworker.domain.MediaEncodeRequestedMessage;
import kr.co.onfilm.encodingworker.infra.coreapi.CoreApiClient;
import kr.co.onfilm.encodingworker.infra.storage.StorageClient;
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
    private final StorageClient storageClient;
    private final FfmpegTranscoder transcoder;

    public void process(MediaEncodeRequestedMessage message) {
        Instant startedAt = Instant.now();
        Path jobDir = Path.of(properties.worker().workingDir()).resolve(message.jobId().toString());
        try {
            log.info("Starting encode job. jobId={}, jobDir={}", message.jobId(), jobDir);

            log.info("Validating encode request. jobId={}", message.jobId());
            validator.validate(message);
            log.info("Validation passed. jobId={}", message.jobId());

            log.info("Reporting PROCESSING status to core api. jobId={}", message.jobId());
            coreApiClient.markProcessing(message.jobId(), startedAt);
            log.info("Reported PROCESSING status. jobId={}", message.jobId());

            Path sourceDestination = jobDir.resolve("source").resolve(sourceFileName(message.sourceKey()));
            log.info(
                    "Downloading source media. jobId={}, source={}/{}, destination={}",
                    message.jobId(),
                    message.sourceBucket(),
                    message.sourceKey(),
                    sourceDestination
            );
            Path sourceFile = storageClient.download(message.sourceBucket(), message.sourceKey(), sourceDestination);
            log.info("Downloaded source media. jobId={}, sourceFile={}", message.jobId(), sourceFile);

            Path outputDir = jobDir.resolve("output");
            log.info("Starting transcode. jobId={}, outputDir={}", message.jobId(), outputDir);
            EncodedOutput output = transcoder.transcode(message, sourceFile, outputDir);
            log.info(
                    "Transcode completed. jobId={}, outputFiles={}, contentType={}",
                    message.jobId(),
                    output.files().size(),
                    output.contentType()
            );

            log.info(
                    "Uploading encoded output. jobId={}, target={}/{}, files={}",
                    message.jobId(),
                    message.targetBucket(),
                    message.targetKey(),
                    output.files().size()
            );
            storageClient.uploadFiles(message.targetBucket(), message.targetKey(), output.files(), output.contentType());
            log.info("Uploaded encoded output. jobId={}", message.jobId());

            log.info(
                    "Updating media path in core api. jobId={}, jobType={}, targetKey={}",
                    message.jobId(),
                    message.jobType(),
                    message.targetKey()
            );
            coreApiClient.updateMediaPath(message.jobType(), message.movieId(), message.jobId(), message.targetKey());
            log.info("Updated media path in core api. jobId={}", message.jobId());

            log.info("Reporting DONE status to core api. jobId={}", message.jobId());
            coreApiClient.markDone(message.jobId(), Instant.now());
            log.info("Completed encode job. jobId={}", message.jobId());
        } catch (JobAlreadyProcessingException e) {
            log.warn("Job is already being processed or completed, skipping. jobId={}", message.jobId());
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
            log.info("Skipping cleanup. jobDir does not exist. jobDir={}", jobDir);
            return;
        }
        log.info("Cleaning up job directory. jobDir={}", jobDir);
        try (var walk = Files.walk(jobDir)) {
            walk.sorted((left, right) -> right.getNameCount() - left.getNameCount())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException ignored) {
                            log.warn("Failed to delete temp path {}", path);
                        }
                    });
            log.info("Cleanup finished. jobDir={}", jobDir);
        } catch (IOException ignored) {
            log.warn("Failed to cleanup working directory {}", jobDir);
        }
    }
}
