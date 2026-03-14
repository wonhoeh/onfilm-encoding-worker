package kr.co.onfilm.encodingworker.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.net.URI;
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
            @NotBlank String workingDir
    ) {
    }

    public record Storage(
            @NotBlank String region
    ) {
    }

    public record CoreApi(
            @NotNull URI baseUrl,
            @NotBlank String mediaJobsPath,
            @NotBlank String moviesPath,
            @NotBlank String trailersPath,
            @NotBlank String authToken
    ) {
    }
}
