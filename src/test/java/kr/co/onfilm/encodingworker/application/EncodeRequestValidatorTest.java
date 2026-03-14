package kr.co.onfilm.encodingworker.application;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.UUID;
import kr.co.onfilm.encodingworker.domain.EncodeJobPreset;
import kr.co.onfilm.encodingworker.domain.EncodeJobType;
import kr.co.onfilm.encodingworker.domain.MediaEncodeRequestedMessage;
import org.junit.jupiter.api.Test;

class EncodeRequestValidatorTest {

    private final EncodeRequestValidator validator = new EncodeRequestValidator();

    @Test
    void acceptsMovieHlsRequest() {
        assertDoesNotThrow(() -> validator.validate(message(
                EncodeJobType.MOVIE,
                EncodeJobPreset.VIDEO_HLS_720P_2500K_AAC_96K,
                "movie/123/file/550e8400-e29b-41d4-a716-446655440000/index.m3u8"
        )));
    }

    @Test
    void rejectsInvalidPresetForTrailer() {
        assertThrows(IllegalArgumentException.class, () -> validator.validate(message(
                EncodeJobType.TRAILER,
                EncodeJobPreset.THUMBNAIL_1280X720,
                "movie/123/trailer/550e8400-e29b-41d4-a716-446655440000/index.m3u8"
        )));
    }

    @Test
    void rejectsInvalidThumbnailTargetKey() {
        assertThrows(IllegalArgumentException.class, () -> validator.validate(message(
                EncodeJobType.THUMBNAIL,
                EncodeJobPreset.THUMBNAIL_1280X720,
                "movie/123/thumbnail/550e8400-e29b-41d4-a716-446655440000/index.m3u8"
        )));
    }

    private MediaEncodeRequestedMessage message(EncodeJobType type, EncodeJobPreset preset, String targetKey) {
        return new MediaEncodeRequestedMessage(
                UUID.randomUUID(),
                123L,
                45L,
                type,
                preset,
                "bucket",
                "movie/123/raw/file/source.mp4",
                "bucket",
                targetKey,
                "video/mp4",
                Instant.parse("2026-03-15T00:00:00Z")
        );
    }
}
