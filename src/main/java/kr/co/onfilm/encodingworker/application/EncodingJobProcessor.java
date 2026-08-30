package kr.co.onfilm.encodingworker.application;

import kr.co.onfilm.encodingworker.config.AppProperties;
import kr.co.onfilm.encodingworker.domain.*;
import kr.co.onfilm.encodingworker.infra.coreapi.*;
import kr.co.onfilm.encodingworker.infra.storage.*;
import kr.co.onfilm.encodingworker.infra.transcode.*;
import kr.co.onfilm.encodingworker.observability.CorrelationIdContext;
import kr.co.onfilm.encodingworker.observability.WorkerMediaEncodeMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;
import java.time.Clock;

import static net.logstash.logback.argument.StructuredArguments.kv;

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
    private final WorkerMediaEncodeMetrics metrics;

    public void process(String kafkaKey, MediaEncodeRequestedMessage message) {
        String correlationId = CorrelationIdContext.resolve(message.correlationId(), message.requestId());
        try (MDC.MDCCloseable ignoredCorrelation = MDC.putCloseable(
                     CorrelationIdContext.MDC_KEY,
                     correlationId
             );
             MDC.MDCCloseable ignoredJob = MDC.putCloseable("jobId", String.valueOf(message.jobId()));
             MDC.MDCCloseable ignoredRequest = MDC.putCloseable("requestId", String.valueOf(message.requestId()))) {
            InboxClaim claim = claimCoordinator.claim(kafkaKey, message);
            metrics.recordInboxClaim(claim);
            if (claim == InboxClaim.TERMINAL) {
                log.info("Skipping terminal inbox job");
                return;
            }
            if (claim == InboxClaim.BUSY) {
                throw new RetryableEncodingException(
                        FailureCode.CORE_API_UNAVAILABLE,
                        "Job is already leased by another worker",
                        null);
            }

            long attemptStartedAt = System.nanoTime();
            Path jobDir = safeJobDirectory(message);
            ProcessingStage stage = ProcessingStage.VALIDATION;
            long stageStartedAt = attemptStartedAt;
            try {
                if (claim == InboxClaim.CALLBACK_ONLY) {
                    stage = ProcessingStage.CALLBACK;
                    stageStartedAt = System.nanoTime();
                    completeCallback(message);
                    metrics.recordStage(
                            message.jobType(), stage.name(), "success",
                            System.nanoTime() - stageStartedAt
                    );
                    markSuccess(message, attemptStartedAt);
                    return;
                }

                validator.validate(kafkaKey, message);
                coreApiClient.markProcessing(message.jobId(), clock.instant());
                metrics.recordStage(
                        message.jobType(), stage.name(), "success",
                        System.nanoTime() - stageStartedAt
                );

                stage = ProcessingStage.DOWNLOAD;
                stageStartedAt = System.nanoTime();
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
                metrics.recordStage(
                        message.jobType(), stage.name(), "success",
                        System.nanoTime() - stageStartedAt
                );

                stage = ProcessingStage.PROBE;
                stageStartedAt = System.nanoTime();
                mediaProbe.validate(sourceFile);
                metrics.recordStage(
                        message.jobType(), stage.name(), "success",
                        System.nanoTime() - stageStartedAt
                );

                stage = ProcessingStage.TRANSCODE;
                stageStartedAt = System.nanoTime();
                EncodedOutput output = transcoder.transcode(message, sourceFile, jobDir.resolve("output"));
                outputValidator.validate(message, output);
                metrics.recordStage(
                        message.jobType(), stage.name(), "success",
                        System.nanoTime() - stageStartedAt
                );

                stage = ProcessingStage.UPLOAD;
                stageStartedAt = System.nanoTime();
                storageClient.uploadFiles(
                        message.targetBucket(), message.targetKey(), output.files(), output.contentType());
                inboxTransactions.markOutputUploaded(message.jobId());
                metrics.recordStage(
                        message.jobType(), stage.name(), "success",
                        System.nanoTime() - stageStartedAt
                );

                stage = ProcessingStage.CALLBACK;
                stageStartedAt = System.nanoTime();
                completeCallback(message);
                metrics.recordStage(
                        message.jobType(), stage.name(), "success",
                        System.nanoTime() - stageStartedAt
                );
                markSuccess(message, attemptStartedAt);
            } catch (RuntimeException exception) {
                RuntimeException classified = classify(exception, stage);
                Failure failure = failure(classified);
                if (!(classified instanceof RetryableEncodingException retryable
                        && retryable.getMessage().contains("already leased"))) {
                    inboxTransactions.recordFailure(
                            message.jobId(), failure.code(), failure.reason(), failure.retryable());
                }
                metrics.recordStage(
                        message.jobType(), stage.name(), "failure",
                        System.nanoTime() - stageStartedAt
                );
                metrics.recordFailure(
                        message.jobType(), stage.name(), failure.code(), failure.retryable()
                );
                metrics.recordAttempt(
                        message.jobType(), "failure", System.nanoTime() - attemptStartedAt
                );
                log.error("Encoding attempt failed. {} {} {} {}",
                        kv("eventType", "MEDIA_ENCODE_ATTEMPT_FAILED"),
                        kv("stage", stage),
                        kv("errorCode", failure.code()),
                        kv("retryable", failure.retryable()),
                        classified);
                throw classified;
            } finally {
                cleanup(jobDir);
            }
        }
    }

    private void completeCallback(MediaEncodeRequestedMessage message) {
        long startedAt = System.nanoTime();
        String result = "error";
        try {
            coreApiClient.complete(
                    message.jobId(), message.targetBucket(), message.targetKey(),
                    message.targetContentType(), clock.instant());
            inboxTransactions.markDone(message.jobId());
            result = "success";
        } catch (CoreApiException exception) {
            result = exception.isRetryable() ? "retry" : "permanent_failure";
            throw exception;
        } finally {
            metrics.recordCallback("complete", result, System.nanoTime() - startedAt);
        }
    }

    private void markSuccess(MediaEncodeRequestedMessage message, long attemptStartedAt) {
        metrics.recordAttempt(
                message.jobType(), "success", System.nanoTime() - attemptStartedAt
        );
        log.info("Completed encode job. {} {} {}",
                kv("eventType", "MEDIA_ENCODE_COMPLETED"),
                kv("jobType", message.jobType()),
                kv("status", "DONE"));
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
