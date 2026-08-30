package kr.co.onfilm.encodingworker.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import java.time.Instant;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record MediaEncodeRequestedMessage(
        int schemaVersion,
        @NotNull UUID jobId,
        @NotNull UUID requestId,
        @Size(max = 64)
        @Pattern(regexp = "^[A-Za-z0-9._-]+$")
        String correlationId,
        @NotNull @Positive Long movieId,
        @NotNull @Positive Long requestedByUserId,
        @NotNull EncodeJobType jobType,
        @NotNull EncodeJobPreset preset,
        @NotBlank @Size(max = 63) String sourceBucket,
        @NotBlank @Size(max = 512) String sourceKey,
        @NotBlank @Size(max = 63) String targetBucket,
        @NotBlank @Size(max = 512) String targetKey,
        @NotBlank @Size(max = 128) String sourceContentType,
        @NotBlank @Size(max = 128) String targetContentType,
        @NotNull Instant requestedAt
) {
    public static final int CURRENT_SCHEMA_VERSION = 1;
}
