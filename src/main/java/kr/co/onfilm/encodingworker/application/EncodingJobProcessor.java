package kr.co.onfilm.encodingworker.application;

import io.micrometer.core.instrument.*;
import kr.co.onfilm.encodingworker.config.AppProperties;
import kr.co.onfilm.encodingworker.domain.*;
import kr.co.onfilm.encodingworker.infra.coreapi.*;
import kr.co.onfilm.encodingworker.infra.storage.*;
import kr.co.onfilm.encodingworker.infra.transcode.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;
import java.time.Clock;

@Slf4j
@Service
@RequiredArgsConstructor
public class EncodingJobProcessor {
    private final AppProperties properties;
    private final EncodeRequestValidator validator;
    private final InboxClaimCoordinator claimCoordinator;
    private final InboxTransactionService inboxTransactions;
    private final CoreApiClient coreApiClient;
    private final StorageClient storageClient;
    private final MediaProbe mediaProbe;
    private final FfmpegTranscoder transcoder;
    private final EncodedOutputValidator outputValidator;
    private final Clock clock;
    private final MeterRegistry meterRegistry;

    public void process(String kafkaKey, MediaEncodeRequestedMessage message) {
        try (MDC.MDCCloseable ignoredJob = MDC.putCloseable("jobId", String.valueOf(message.jobId()));
             MDC.MDCCloseable ignoredRequest = MDC.putCloseable("requestId", String.valueOf(message.requestId()))) {
            InboxClaim claim = claimCoordinator.claim(kafkaKey, message);
            if (claim == InboxClaim.TERMINAL) {
                meterRegistry.counter("media.encode.duplicate", "result", "terminal").increment();
                log.info("Skipping terminal inbox job");
                return;
            }
            if (claim == InboxClaim.BUSY) {
                meterRegistry.counter("media.encode.duplicate", "result", "busy").increment();
                throw new RetryableEncodingException(
                        FailureCode.CORE_API_UNAVAILABLE,
                        "Job is already leased by another worker",
                        null);
            }

            Timer.Sample total = Timer.start(meterRegistry);
            Path jobDir = safeJobDirectory(message);
            ProcessingStage stage = ProcessingStage.VALIDATION;
            try {
                if (claim == InboxClaim.CALLBACK_ONLY) {
                    completeCallback(message);
                    markSuccess(message, total);
                    return;
                }

                validator.validate(kafkaKey, message);
                coreApiClient.markProcessing(message.jobId(), clock.instant());

                stage = ProcessingStage.DOWNLOAD;
                StorageObjectMetadata metadata =
                        storageClient.metadata(message.sourceBucket(), message.sourceKey());
                if (metadata.contentLength() <= 0
                        || metadata.contentLength() > properties.worker().maxSourceBytes()) {
                    throw new PermanentEncodingException(
                            FailureCode.UNSUPPORTED_MEDIA, "Source size violates worker policy");
                }
                if (metadata.contentType() != null && !metadata.contentType().isBlank()
                        && !metadata.contentType().equalsIgnoreCase(message.sourceContentType())) {
                    throw new PermanentEncodingException(
                            FailureCode.UNSUPPORTED_MEDIA,
                            "Source content type does not match the upload request");
                }
                Path sourceDestination = jobDir.resolve("source").resolve(sourceFileName(message.sourceKey()));
                Path sourceFile = storageClient.download(
                        message.sourceBucket(), message.sourceKey(), sourceDestination);

                stage = ProcessingStage.PROBE;
                mediaProbe.validate(sourceFile);

                stage = ProcessingStage.TRANSCODE;
                EncodedOutput output = transcoder.transcode(message, sourceFile, jobDir.resolve("output"));
                outputValidator.validate(message, output);

                stage = ProcessingStage.UPLOAD;
                storageClient.uploadFiles(
                        message.targetBucket(), message.targetKey(), output.files(), output.contentType());
                inboxTransactions.markOutputUploaded(message.jobId());

                stage = ProcessingStage.CALLBACK;
                completeCallback(message);
                markSuccess(message, total);
            } catch (RuntimeException exception) {
                RuntimeException classified = classify(exception, stage);
                if (!(classified instanceof RetryableEncodingException retryable
                        && retryable.getMessage().contains("already leased"))) {
                    Failure failure = failure(classified);
                    inboxTransactions.recordFailure(
                            message.jobId(), failure.code(), failure.reason(), failure.retryable());
                }
                meterRegistry.counter("media.encode.failed",
                        "stage", stage.name().toLowerCase(),
                        "retryable", Boolean.toString(failure(classified).retryable())).increment();
                total.stop(meterRegistry.timer("media.encode.duration", "result", "failed"));
                log.error("Encoding attempt failed. stage={}", stage, classified);
                throw classified;
            } finally {
                cleanup(jobDir);
            }
        }
    }

    private void completeCallback(MediaEncodeRequestedMessage message) {
        coreApiClient.complete(
                message.jobId(), message.targetBucket(), message.targetKey(),
                message.targetContentType(), clock.instant());
        inboxTransactions.markDone(message.jobId());
    }

    private void markSuccess(MediaEncodeRequestedMessage message, Timer.Sample total) {
        meterRegistry.counter("media.encode.completed", "type", message.jobType().name()).increment();
        total.stop(meterRegistry.timer("media.encode.duration", "result", "success"));
        log.info("Completed encode job. type={}, target={}/{}",
                message.jobType(), message.targetBucket(), message.targetKey());
    }

    private RuntimeException classify(RuntimeException exception, ProcessingStage stage) {
        if (exception instanceof PermanentEncodingException
                || exception instanceof RetryableEncodingException) return exception;
        if (exception instanceof CoreApiException core) {
            return core.isRetryable()
                    ? new RetryableEncodingException(FailureCode.CORE_API_UNAVAILABLE, core.getMessage(), core)
                    : new PermanentEncodingException(FailureCode.INVALID_REQUEST, core.getMessage(), core);
        }
        if (exception instanceof StorageException storage) {
            FailureCode code = stage == ProcessingStage.UPLOAD
                    ? FailureCode.OUTPUT_UPLOAD_FAILED
                    : FailureCode.SOURCE_DOWNLOAD_FAILED;
            return storage.isRetryable()
                    ? new RetryableEncodingException(code, storage.getMessage(), storage)
                    : new PermanentEncodingException(
                            stage == ProcessingStage.DOWNLOAD ? FailureCode.SOURCE_NOT_FOUND : code,
                            storage.getMessage(), storage);
        }
        if (exception instanceof TranscodeTimeoutException timeout) {
            return new RetryableEncodingException(FailureCode.ENCODE_TIMEOUT, timeout.getMessage(), timeout);
        }
        if (exception instanceof TranscodeException transcode) {
            return new PermanentEncodingException(FailureCode.ENCODE_FAILED, transcode.getMessage(), transcode);
        }
        return new RetryableEncodingException(
                FailureCode.UNEXPECTED_WORKER_ERROR, "Unexpected worker error", exception);
    }

    private Failure failure(RuntimeException exception) {
        FailureCode code;
        boolean retryable;
        if (exception instanceof PermanentEncodingException permanent) {
            code = permanent.getFailureCode();
            retryable = false;
        } else if (exception instanceof RetryableEncodingException transientFailure) {
            code = transientFailure.getFailureCode();
            retryable = true;
        } else {
            code = FailureCode.UNEXPECTED_WORKER_ERROR;
            retryable = true;
        }
        String root = ExceptionUtils.getRootCauseMessage(exception);
        return new Failure(code, CoreApiClient.sanitizeReason(root), retryable);
    }

    private Path safeJobDirectory(MediaEncodeRequestedMessage message) {
        Path root = Path.of(properties.worker().workingDir()).toAbsolutePath().normalize();
        Path jobDir = root.resolve(message.jobId().toString()).normalize();
        if (!jobDir.startsWith(root)) throw new SecurityException("Job directory escapes worker root");
        return jobDir;
    }

    private String sourceFileName(String sourceKey) {
        return Path.of(sourceKey).getFileName().toString();
    }

    private void cleanup(Path jobDir) {
        if (Files.notExists(jobDir)) return;
        try (var walk = Files.walk(jobDir)) {
            walk.sorted((left, right) -> right.getNameCount() - left.getNameCount())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException exception) {
                            log.warn("Failed to delete temp path {}", path, exception);
                        }
                    });
        } catch (IOException exception) {
            log.warn("Failed to cleanup working directory {}", jobDir, exception);
        }
    }

    private enum ProcessingStage {
        VALIDATION, DOWNLOAD, PROBE, TRANSCODE, UPLOAD, CALLBACK
    }

    private record Failure(FailureCode code, String reason, boolean retryable) {
    }
}
