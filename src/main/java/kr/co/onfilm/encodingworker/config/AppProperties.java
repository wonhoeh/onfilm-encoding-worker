package kr.co.onfilm.encodingworker.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.net.URI;
import java.time.Duration;
import java.util.Set;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Size;
import jakarta.validation.constraints.AssertTrue;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "app")
public record AppProperties(
        @Valid @NotNull Worker worker,
        @Valid @NotNull Storage storage,
        @Valid @NotNull CoreApi coreApi
) {

    public record Worker(
            @NotBlank String topic,
            @NotBlank String groupId,
            @NotBlank String ffmpegPath,
            @NotBlank String ffprobePath,
            @NotBlank String workingDir,
            @NotNull Duration transcodeTimeout,
            @NotNull Duration processingLease,
            @Min(1) long maxSourceBytes,
            @NotNull Duration maxMediaDuration,
            @Min(1) int concurrency,
            @Min(2) int retryAttempts,
            @Min(1) long retryDelayMillis,
            @DecimalMin("1.0") double retryMultiplier,
            @Min(1) long failureReportDelay,
            @Min(1) long staleRecoveryDelay
    ) {
        @AssertTrue(message = "worker durations must be positive and processingLease must exceed transcodeTimeout")
        public boolean hasValidDurations() {
            return positive(transcodeTimeout)
                    && positive(processingLease)
                    && positive(maxMediaDuration)
                    && processingLease.compareTo(transcodeTimeout) > 0;
        }
    }

    public record Storage(
            @NotBlank String type,
            String region,
            String localRoot,
            @NotNull Set<@NotBlank String> allowedBuckets,
            @NotNull Duration apiCallTimeout,
            @NotNull Duration apiCallAttemptTimeout
    ) {
        @AssertTrue(message = "storage configuration does not match storage type")
        public boolean isValidForType() {
            if (type == null) return false;
            return switch (type.toLowerCase()) {
                case "s3" -> region != null && !region.isBlank() && !allowedBuckets.isEmpty()
                        && positive(apiCallTimeout) && positive(apiCallAttemptTimeout);
                case "local" -> localRoot != null && !localRoot.isBlank() && !allowedBuckets.isEmpty()
                        && positive(apiCallTimeout) && positive(apiCallAttemptTimeout);
                default -> false;
            };
        }
    }

    public record CoreApi(
            @NotNull URI baseUrl,
            @NotBlank String processingPath,
            @NotBlank String completionPath,
            @NotBlank String failurePath,
            @NotBlank @Size(min = 32) String callbackSecret,
            @NotNull Duration connectTimeout,
            @NotNull Duration readTimeout
    ) {
        @AssertTrue(message = "core api timeouts must be positive")
        public boolean hasValidTimeouts() {
            return positive(connectTimeout) && positive(readTimeout);
        }
    }

    private static boolean positive(Duration duration) {
        return duration != null && !duration.isNegative() && !duration.isZero();
    }
}
