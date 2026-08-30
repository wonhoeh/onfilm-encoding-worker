package kr.co.onfilm.encodingworker.application;

import jakarta.validation.Validation;
import kr.co.onfilm.encodingworker.TestProperties;
import kr.co.onfilm.encodingworker.domain.*;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

class EncodeRequestValidatorTest {
    private EncodeRequestValidator validator;

    @BeforeEach
    void setUp() {
        validator = new EncodeRequestValidator(
                TestProperties.create(),
                Validation.buildDefaultValidatorFactory().getValidator());
    }

    @Test
    void acceptsServerV1MovieMessage() {
        MediaEncodeRequestedMessage message = message(EncodeJobType.MOVIE,
                EncodeJobPreset.VIDEO_HLS_720P_2500K_AAC_96K);
        assertThatCode(() -> validator.validate(message.jobId().toString(), message))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsUnsupportedSchemaAndKafkaKeyMismatch() {
        MediaEncodeRequestedMessage valid = message(EncodeJobType.MOVIE,
                EncodeJobPreset.VIDEO_HLS_720P_2500K_AAC_96K);
        MediaEncodeRequestedMessage unsupported = copy(valid, 2, valid.sourceKey());

        assertThatThrownBy(() -> validator.validate(valid.jobId().toString(), unsupported))
                .isInstanceOf(PermanentEncodingException.class)
                .extracting("failureCode").isEqualTo(FailureCode.UNSUPPORTED_MESSAGE_SCHEMA);
        assertThatThrownBy(() -> validator.validate(UUID.randomUUID().toString(), valid))
                .isInstanceOf(PermanentEncodingException.class);
    }

    @Test
    void rejectsSourceKeyThatDoesNotContainRequestIdAndTraversal() {
        MediaEncodeRequestedMessage valid = message(EncodeJobType.MOVIE,
                EncodeJobPreset.VIDEO_HLS_720P_2500K_AAC_96K);
        assertThatThrownBy(() -> validator.validate(valid.jobId().toString(),
                copy(valid, 1, "movie/123/raw/file/../source.mp4")))
                .isInstanceOf(PermanentEncodingException.class);
    }

    @Test
    void rejectsPresetAndContentTypeMismatch() {
        MediaEncodeRequestedMessage thumbnail = message(
                EncodeJobType.THUMBNAIL, EncodeJobPreset.VIDEO_HLS_720P_2500K_AAC_96K);
        assertThatThrownBy(() -> validator.validate(thumbnail.jobId().toString(), thumbnail))
                .isInstanceOf(PermanentEncodingException.class);
    }

    @Test
    void rejectsTargetWithoutGeneratedUuid() {
        MediaEncodeRequestedMessage valid = message(EncodeJobType.MOVIE,
                EncodeJobPreset.VIDEO_HLS_720P_2500K_AAC_96K);
        MediaEncodeRequestedMessage invalid = copyTarget(
                valid, "movie/123/file/not-a-uuid/index.m3u8");

        assertThatThrownBy(() -> validator.validate(valid.jobId().toString(), invalid))
                .isInstanceOf(PermanentEncodingException.class);
    }

    private MediaEncodeRequestedMessage message(EncodeJobType type, EncodeJobPreset preset) {
        UUID jobId = UUID.randomUUID();
        UUID requestId = UUID.randomUUID();
        String mediaType = switch (type) {
            case MOVIE -> "file";
            case TRAILER -> "trailer";
            case THUMBNAIL -> "thumbnail";
        };
        String target = type == EncodeJobType.THUMBNAIL
                ? "movie/123/thumbnail/" + UUID.randomUUID() + ".jpg"
                : "movie/123/" + mediaType + "/" + UUID.randomUUID() + "/index.m3u8";
        return new MediaEncodeRequestedMessage(
                1, jobId, requestId, "corr-123", 123L, 45L, type, preset,
                "bucket", "movie/123/raw/" + mediaType + "/" + requestId + ".mp4",
                "bucket", target,
                type == EncodeJobType.THUMBNAIL ? "image/jpeg" : "video/mp4",
                type == EncodeJobType.THUMBNAIL ? "image/jpeg" : "application/vnd.apple.mpegurl",
                Instant.parse("2026-03-15T00:00:00Z"));
    }

    private MediaEncodeRequestedMessage copyTarget(MediaEncodeRequestedMessage source, String targetKey) {
        return new MediaEncodeRequestedMessage(
                source.schemaVersion(), source.jobId(), source.requestId(), source.correlationId(), source.movieId(),
                source.requestedByUserId(), source.jobType(), source.preset(),
                source.sourceBucket(), source.sourceKey(), source.targetBucket(), targetKey,
                source.sourceContentType(), source.targetContentType(), source.requestedAt());
    }

    private MediaEncodeRequestedMessage copy(MediaEncodeRequestedMessage source,
                                               int schemaVersion, String sourceKey) {
        return new MediaEncodeRequestedMessage(
                schemaVersion, source.jobId(), source.requestId(), source.correlationId(), source.movieId(),
                source.requestedByUserId(), source.jobType(), source.preset(),
                source.sourceBucket(), sourceKey, source.targetBucket(), source.targetKey(),
                source.sourceContentType(), source.targetContentType(), source.requestedAt());
    }
}
