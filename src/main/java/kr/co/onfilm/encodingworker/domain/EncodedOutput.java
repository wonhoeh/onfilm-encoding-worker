package kr.co.onfilm.encodingworker.domain;

import java.nio.file.Path;
import java.util.List;

public record EncodedOutput(
        String contentType,
        List<Path> files
) {
    public EncodedOutput {
        if (contentType == null || contentType.isBlank()) {
            throw new IllegalArgumentException("contentType is required");
        }
        files = files == null ? List.of() : List.copyOf(files);
    }
}
