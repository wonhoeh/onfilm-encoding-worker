package kr.co.onfilm.encodingworker.infra.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.*;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class StoragePathsTest {
    @TempDir Path root;

    @Test
    void rejectsPathTraversal() {
        assertThatThrownBy(() -> StoragePaths.resolveBelow(root, "bucket", "../../outside"))
                .isInstanceOf(StorageException.class);
    }

    @Test
    void ordersHlsManifestAfterSegments() {
        List<Path> ordered = StoragePaths.manifestLast(List.of(
                Path.of("index.m3u8"), Path.of("segment_001.ts"), Path.of("segment_000.ts")));
        assertThat(ordered).extracting(path -> path.getFileName().toString())
                .containsExactly("segment_000.ts", "segment_001.ts", "index.m3u8");
    }
}
