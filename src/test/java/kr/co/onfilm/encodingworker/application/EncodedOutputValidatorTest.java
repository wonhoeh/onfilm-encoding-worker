package kr.co.onfilm.encodingworker.application;

import kr.co.onfilm.encodingworker.domain.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.*;
import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

class EncodedOutputValidatorTest {
    private final EncodedOutputValidator validator = new EncodedOutputValidator();
    @TempDir Path tempDir;

    @Test
    void acceptsCompleteHlsOutput() throws Exception {
        Path manifest = Files.writeString(tempDir.resolve("index.m3u8"),
                "#EXTM3U\n#EXTINF:6,\nsegment_000.ts\n");
        Path segment = Files.writeString(tempDir.resolve("segment_000.ts"), "video");
        assertThatCode(() -> validator.validate(movieMessage(),
                new EncodedOutput("application/vnd.apple.mpegurl", List.of(manifest, segment))))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsMissingSegmentAndEmptyOutput() throws Exception {
        Path manifest = Files.writeString(tempDir.resolve("index.m3u8"),
                "#EXTM3U\nmissing.ts\n");
        assertThatThrownBy(() -> validator.validate(movieMessage(),
                new EncodedOutput("application/vnd.apple.mpegurl", List.of(manifest))))
                .isInstanceOf(PermanentEncodingException.class)
                .extracting("failureCode").isEqualTo(FailureCode.OUTPUT_VALIDATION_FAILED);
    }

    @Test
    void rejectsTargetContentTypeMismatch() throws Exception {
        Path image = Files.writeString(tempDir.resolve("image.jpg"), "image");
        assertThatThrownBy(() -> validator.validate(movieMessage(),
                new EncodedOutput("image/jpeg", List.of(image))))
                .isInstanceOf(PermanentEncodingException.class);
    }

    private MediaEncodeRequestedMessage movieMessage() {
        UUID jobId = UUID.randomUUID();
        UUID requestId = UUID.randomUUID();
        return new MediaEncodeRequestedMessage(
                1, jobId, requestId, 1L, 2L,
                EncodeJobType.MOVIE, EncodeJobPreset.VIDEO_HLS_720P_2500K_AAC_96K,
                "bucket", "movie/1/raw/file/" + requestId + ".mp4",
                "bucket", "movie/1/file/" + jobId + "/index.m3u8",
                "video/mp4", "application/vnd.apple.mpegurl", Instant.now());
    }
}
