package kr.co.onfilm.encodingworker.infra.transcode;

import kr.co.onfilm.encodingworker.application.*;
import kr.co.onfilm.encodingworker.config.AppProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
public class MediaProbe {
    private static final Duration PROBE_TIMEOUT = Duration.ofSeconds(30);
    private final AppProperties properties;

    public void validate(Path source) {
        Process process = null;
        Path probeOutput = source.resolveSibling(source.getFileName() + ".ffprobe.log");
        try {
            process = new ProcessBuilder(
                    properties.worker().ffprobePath(),
                    "-v", "error",
                    "-show_entries", "format=duration:stream=codec_type,width,height",
                    "-of", "default=noprint_wrappers=1",
                    source.toString())
                    .redirectErrorStream(true)
                    .redirectOutput(probeOutput.toFile())
                    .start();
            if (!process.waitFor(PROBE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                process.destroyForcibly();
                throw new PermanentEncodingException(FailureCode.UNSUPPORTED_MEDIA, "ffprobe timed out");
            }
            String metadata = readBounded(probeOutput);
            if (process.exitValue() != 0 || !metadata.contains("codec_type=video")) {
                throw new PermanentEncodingException(FailureCode.UNSUPPORTED_MEDIA,
                        "Source does not contain a supported visual stream");
            }
            parseDuration(metadata);
        } catch (IOException exception) {
            throw new RetryableEncodingException(
                    FailureCode.UNEXPECTED_WORKER_ERROR, "Failed to run ffprobe", exception);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            if (process != null) process.destroyForcibly();
            throw new RetryableEncodingException(
                    FailureCode.UNEXPECTED_WORKER_ERROR, "ffprobe was interrupted", exception);
        } finally {
            try {
                Files.deleteIfExists(probeOutput);
            } catch (IOException ignored) {
                // The job directory cleanup will retry removal.
            }
        }
    }

    private String readBounded(Path output) throws IOException {
        try (var input = Files.newInputStream(output)) {
            return new String(input.readNBytes(64 * 1024), StandardCharsets.UTF_8);
        }
    }

    private void parseDuration(String metadata) {
        metadata.lines()
                .filter(line -> line.startsWith("duration="))
                .map(line -> line.substring("duration=".length()))
                .filter(value -> !value.equalsIgnoreCase("N/A"))
                .findFirst()
                .ifPresent(value -> {
                    try {
                        double seconds = Double.parseDouble(value);
                        if (seconds > properties.worker().maxMediaDuration().toSeconds()) {
                            throw new PermanentEncodingException(
                                    FailureCode.UNSUPPORTED_MEDIA, "Source duration exceeds policy");
                        }
                    } catch (NumberFormatException exception) {
                        throw new PermanentEncodingException(
                                FailureCode.UNSUPPORTED_MEDIA, "Invalid media duration", exception);
                    }
                });
    }
}
