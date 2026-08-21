package kr.co.onfilm.encodingworker.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MediaEncodeRequestedMessageTest {
    @Test
    void deserializesCurrentApiServerMessage() throws Exception {
        String json = """
                {
                  "schemaVersion": 1,
                  "jobId": "11111111-2222-3333-4444-555555555555",
                  "requestId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                  "movieId": 10,
                  "requestedByUserId": 7,
                  "jobType": "MOVIE",
                  "preset": "VIDEO_HLS_720P_2500K_AAC_96K",
                  "sourceBucket": "bucket",
                  "sourceKey": "movie/10/raw/file/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.mp4",
                  "targetBucket": "bucket",
                  "targetKey": "movie/10/file/11111111-2222-3333-4444-555555555555/index.m3u8",
                  "sourceContentType": "video/mp4",
                  "targetContentType": "application/vnd.apple.mpegurl",
                  "requestedAt": "2026-08-21T00:00:00Z"
                }
                """;
        MediaEncodeRequestedMessage message =
                new ObjectMapper().findAndRegisterModules().readValue(json, MediaEncodeRequestedMessage.class);
        assertThat(message.schemaVersion()).isEqualTo(1);
        assertThat(message.requestId().toString()).isEqualTo("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        assertThat(message.targetContentType()).isEqualTo("application/vnd.apple.mpegurl");
    }
}
