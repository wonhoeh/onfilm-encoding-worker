package kr.co.onfilm.encodingworker.application;

import kr.co.onfilm.encodingworker.domain.*;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

@Component
public class EncodedOutputValidator {
    public void validate(MediaEncodeRequestedMessage message, EncodedOutput output) {
        if (!message.targetContentType().equals(output.contentType()) || output.files().isEmpty()) {
            throw invalid("Encoded output content type or file list is invalid");
        }
        output.files().forEach(file -> {
            try {
                if (!Files.isRegularFile(file) || Files.size(file) <= 0) {
                    throw invalid("Encoded output file is empty: " + file.getFileName());
                }
            } catch (IOException exception) {
                throw new PermanentEncodingException(
                        FailureCode.OUTPUT_VALIDATION_FAILED, "Unable to inspect encoded output", exception);
            }
        });
        if (message.jobType() == EncodeJobType.THUMBNAIL) {
            if (output.files().size() != 1 || !output.files().get(0).toString().endsWith(".jpg")) {
                throw invalid("Thumbnail output must contain exactly one jpg");
            }
            return;
        }
        validatePlaylist(output.files());
    }

    private void validatePlaylist(List<Path> files) {
        Path manifest = files.stream()
                .filter(path -> path.getFileName().toString().equals("index.m3u8"))
                .findFirst().orElseThrow(() -> invalid("HLS manifest is missing"));
        Set<String> names = files.stream()
                .map(path -> path.getFileName().toString())
                .collect(java.util.stream.Collectors.toSet());
        try {
            List<String> segments = Files.readAllLines(manifest).stream()
                    .map(String::trim)
                    .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                    .toList();
            if (segments.isEmpty() || segments.stream().anyMatch(segment ->
                    segment.contains("/") || segment.contains("..") || !names.contains(segment))) {
                throw invalid("HLS manifest references a missing or unsafe segment");
            }
        } catch (IOException exception) {
            throw new PermanentEncodingException(
                    FailureCode.OUTPUT_VALIDATION_FAILED, "Unable to read HLS manifest", exception);
        }
    }

    private PermanentEncodingException invalid(String message) {
        return new PermanentEncodingException(FailureCode.OUTPUT_VALIDATION_FAILED, message);
    }
}
