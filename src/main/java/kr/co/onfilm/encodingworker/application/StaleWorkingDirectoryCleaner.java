package kr.co.onfilm.encodingworker.application;

import kr.co.onfilm.encodingworker.config.AppProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.*;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class StaleWorkingDirectoryCleaner {
    private final AppProperties properties;
    private final Clock clock;

    @EventListener(ApplicationReadyEvent.class)
    public void cleanup() {
        Path root = Path.of(properties.worker().workingDir()).toAbsolutePath().normalize();
        if (Files.notExists(root)) return;
        Instant cutoff = clock.instant().minus(properties.worker().processingLease().multipliedBy(2));
        try (var children = Files.list(root)) {
            children.filter(Files::isDirectory)
                    .filter(this::hasUuidName)
                    .filter(path -> olderThan(path, cutoff))
                    .forEach(this::deleteTree);
        } catch (IOException exception) {
            log.warn("Failed to scan stale worker directories. root={}", root, exception);
        }
    }

    private boolean hasUuidName(Path path) {
        try {
            UUID.fromString(path.getFileName().toString());
            return true;
        } catch (IllegalArgumentException exception) {
            return false;
        }
    }

    private boolean olderThan(Path path, Instant cutoff) {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class)
                    .lastModifiedTime().toInstant().isBefore(cutoff);
        } catch (IOException exception) {
            return false;
        }
    }

    private void deleteTree(Path directory) {
        try (var walk = Files.walk(directory)) {
            walk.sorted((left, right) -> right.getNameCount() - left.getNameCount())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException exception) {
                            log.warn("Failed to delete stale path {}", path, exception);
                        }
                    });
        } catch (IOException exception) {
            log.warn("Failed to cleanup stale directory {}", directory, exception);
        }
    }
}
