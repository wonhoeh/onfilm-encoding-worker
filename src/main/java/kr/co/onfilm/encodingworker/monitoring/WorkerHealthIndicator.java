package kr.co.onfilm.encodingworker.monitoring;

import kr.co.onfilm.encodingworker.config.AppProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.health.*;
import org.springframework.stereotype.Component;

import java.nio.file.*;

@Component
@RequiredArgsConstructor
public class WorkerHealthIndicator implements HealthIndicator {
    private static final long MIN_USABLE_BYTES = 1024L * 1024 * 1024;
    private final AppProperties properties;

    @Override
    public Health health() {
        try {
            Path root = Path.of(properties.worker().workingDir()).toAbsolutePath().normalize();
            Files.createDirectories(root);
            long usable = Files.getFileStore(root).getUsableSpace();
            if (!Files.isWritable(root) || usable < MIN_USABLE_BYTES) {
                return Health.down()
                        .withDetail("workingDir", root)
                        .withDetail("usableBytes", usable)
                        .withDetail("reason", "working directory is not writable or disk space is low")
                        .build();
            }
            String unavailableBinary = unavailableAbsoluteBinary();
            if (unavailableBinary != null) {
                return Health.down().withDetail(
                        "reason", unavailableBinary + " is not executable").build();
            }
            return Health.up()
                    .withDetail("workingDir", root)
                    .withDetail("usableBytes", usable)
                    .build();
        } catch (Exception exception) {
            return Health.down(exception).build();
        }
    }

    private String unavailableAbsoluteBinary() {
        for (String binary : new String[]{
                properties.worker().ffmpegPath(), properties.worker().ffprobePath()}) {
            Path path = Path.of(binary);
            if (path.isAbsolute() && !Files.isExecutable(path)) return binary;
        }
        return null;
    }
}
