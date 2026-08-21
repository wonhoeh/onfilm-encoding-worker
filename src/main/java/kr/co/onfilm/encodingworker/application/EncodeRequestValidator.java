package kr.co.onfilm.encodingworker.application;

import kr.co.onfilm.encodingworker.config.AppProperties;
import kr.co.onfilm.encodingworker.domain.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.regex.Pattern;
import java.util.UUID;
import jakarta.validation.Validator;

@Component
@RequiredArgsConstructor
public class EncodeRequestValidator {
    private static final Pattern CLEAN_KEY = Pattern.compile("^[A-Za-z0-9._/-]+$");
    private final AppProperties properties;
    private final Validator beanValidator;

    public void validate(String kafkaKey, MediaEncodeRequestedMessage message) {
        var violations = beanValidator.validate(message);
        if (!violations.isEmpty()) {
            throw new PermanentEncodingException(FailureCode.INVALID_REQUEST,
                    "Message validation failed: " + violations.iterator().next().getPropertyPath());
        }
        if (message.schemaVersion() != MediaEncodeRequestedMessage.CURRENT_SCHEMA_VERSION) {
            throw new PermanentEncodingException(FailureCode.UNSUPPORTED_MESSAGE_SCHEMA,
                    "Unsupported message schemaVersion: " + message.schemaVersion());
        }
        if (kafkaKey == null || !kafkaKey.equals(message.jobId().toString())) {
            throw new PermanentEncodingException(FailureCode.INVALID_REQUEST,
                    "Kafka key must match payload jobId");
        }
        if (!properties.storage().allowedBuckets().contains(message.sourceBucket())
                || !properties.storage().allowedBuckets().contains(message.targetBucket())) {
            throw new PermanentEncodingException(FailureCode.INVALID_REQUEST, "Bucket is not allowed");
        }

        boolean validPreset = switch (message.jobType()) {
            case MOVIE, TRAILER -> message.preset() == EncodeJobPreset.VIDEO_HLS_720P_2500K_AAC_96K;
            case THUMBNAIL -> message.preset() == EncodeJobPreset.THUMBNAIL_1280X720;
        };
        if (!validPreset) {
            throw new PermanentEncodingException(FailureCode.INVALID_REQUEST,
                    "Invalid preset for job type: " + message.jobType() + "/" + message.preset());
        }

        requireCleanKey(message.sourceKey(), "sourceKey");
        requireCleanKey(message.targetKey(), "targetKey");
        if (message.sourceBucket().equals(message.targetBucket())
                && message.sourceKey().equals(message.targetKey())) {
            throw new PermanentEncodingException(FailureCode.INVALID_REQUEST,
                    "Source and target must be different");
        }

        String mediaType = switch (message.jobType()) {
            case MOVIE -> "file";
            case TRAILER -> "trailer";
            case THUMBNAIL -> "thumbnail";
        };
        String sourcePrefix = "movie/%d/raw/%s/%s.".formatted(
                message.movieId(), mediaType, message.requestId());
        if (!message.sourceKey().startsWith(sourcePrefix)
                || message.sourceKey().substring(sourcePrefix.length()).contains("/")) {
            throw new PermanentEncodingException(FailureCode.INVALID_REQUEST,
                    "sourceKey does not match requestId, movieId and jobType");
        }

        switch (message.jobType()) {
            case MOVIE -> {
                requireVideoTarget(message.targetKey(),
                        "movie/%d/file/".formatted(message.movieId()));
                requireContentTypes(message, "video/", "application/vnd.apple.mpegurl");
            }
            case TRAILER -> {
                requireVideoTarget(message.targetKey(),
                        "movie/%d/trailer/".formatted(message.movieId()));
                requireContentTypes(message, "video/", "application/vnd.apple.mpegurl");
            }
            case THUMBNAIL -> {
                requireThumbnailTarget(message.targetKey(),
                        "movie/%d/thumbnail/".formatted(message.movieId()));
                requireContentTypes(message, "image/", "image/jpeg");
            }
        }
    }

    private void requireContentTypes(MediaEncodeRequestedMessage message,
                                     String sourcePrefix, String expectedTarget) {
        if (!message.sourceContentType().startsWith(sourcePrefix)
                || !message.targetContentType().equals(expectedTarget)) {
            throw new PermanentEncodingException(FailureCode.INVALID_REQUEST,
                    "Content types do not match jobType");
        }
    }

    private void requireVideoTarget(String targetKey, String prefix) {
        String suffix = "/index.m3u8";
        if (!targetKey.startsWith(prefix) || !targetKey.endsWith(suffix)) invalidTarget(targetKey);
        requireUuid(targetKey.substring(prefix.length(), targetKey.length() - suffix.length()), targetKey);
    }

    private void requireThumbnailTarget(String targetKey, String prefix) {
        String suffix = ".jpg";
        if (!targetKey.startsWith(prefix) || !targetKey.endsWith(suffix)) invalidTarget(targetKey);
        requireUuid(targetKey.substring(prefix.length(), targetKey.length() - suffix.length()), targetKey);
    }

    private void requireUuid(String value, String targetKey) {
        try {
            if (!UUID.fromString(value).toString().equals(value)) invalidTarget(targetKey);
        } catch (IllegalArgumentException exception) {
            invalidTarget(targetKey);
        }
    }

    private void invalidTarget(String targetKey) {
        throw new PermanentEncodingException(FailureCode.INVALID_REQUEST,
                "Invalid targetKey: " + targetKey);
    }

    private void requireCleanKey(String key, String field) {
        if (key.startsWith("/") || key.contains("\\") || key.contains("..")
                || !CLEAN_KEY.matcher(key).matches()) {
            throw new PermanentEncodingException(FailureCode.INVALID_REQUEST, "Invalid " + field);
        }
    }
}
