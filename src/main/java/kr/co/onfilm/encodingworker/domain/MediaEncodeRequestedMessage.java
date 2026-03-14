package kr.co.onfilm.encodingworker.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record MediaEncodeRequestedMessage(
        @NotNull UUID jobId,
        @NotNull Long movieId,
        @NotNull Long requestedByUserId,
        @NotNull EncodeJobType jobType,
        @NotNull EncodeJobPreset preset,
        @NotBlank String sourceBucket,
        @NotBlank String sourceKey,
        @NotBlank String targetBucket,
        @NotBlank String targetKey,
        @NotBlank String contentType,
        @NotNull Instant requestedAt
) {
}
