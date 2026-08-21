package kr.co.onfilm.encodingworker.infra.storage;

import java.nio.file.Path;
import java.util.*;

final class StoragePaths {
    private StoragePaths() {
    }

    static Path resolveBelow(Path root, String bucket, String key) {
        Path normalizedRoot = root.toAbsolutePath().normalize();
        Path resolved = normalizedRoot.resolve(bucket).resolve(key).normalize();
        if (!resolved.startsWith(normalizedRoot)) throw new StorageException("Storage path escapes configured root");
        return resolved;
    }

    static List<Path> manifestLast(List<Path> files) {
        return files.stream()
                .sorted(Comparator
                        .comparing((Path path) -> path.getFileName().toString().endsWith(".m3u8"))
                        .thenComparing(path -> path.getFileName().toString()))
                .toList();
    }
}
