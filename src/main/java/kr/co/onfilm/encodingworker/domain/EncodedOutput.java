package kr.co.onfilm.encodingworker.domain;

import java.nio.file.Path;
import java.util.List;

public record EncodedOutput(
        String contentType,
        List<Path> files
) {
}
