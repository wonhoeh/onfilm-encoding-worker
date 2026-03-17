package kr.co.onfilm.encodingworker.infra.transcode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import kr.co.onfilm.encodingworker.config.AppProperties;
import kr.co.onfilm.encodingworker.domain.EncodeJobPreset;
import org.junit.jupiter.api.Test;

class FfmpegCommandFactoryTest {

    @Test
    void buildsHlsCommandWithManifestTarget() {
        FfmpegCommandFactory factory = new FfmpegCommandFactory(properties());

        List<String> command = factory.create(
                EncodeJobPreset.VIDEO_HLS_720P_2500K_AAC_96K,
                Path.of("/tmp/source.mp4"),
                Path.of("/tmp/movie/123/file/uuid/index.m3u8")
        );

        assertEquals("ffmpeg", command.get(0));
        assertTrue(command.contains("-f"));
        assertTrue(command.contains("hls"));
        assertTrue(command.contains("/tmp/movie/123/file/uuid/index.m3u8"));
        assertTrue(command.contains("/tmp/movie/123/file/uuid/segment_%03d.ts"));
    }

    @Test
    void buildsThumbnailCommand() {
        FfmpegCommandFactory factory = new FfmpegCommandFactory(properties());

        List<String> command = factory.create(
                EncodeJobPreset.THUMBNAIL_1280X720,
                Path.of("/tmp/source.mp4"),
                Path.of("/tmp/movie/123/thumbnail/uuid.jpg")
        );

        assertEquals("ffmpeg", command.get(0));
        assertTrue(command.contains("-frames:v"));
        assertTrue(command.contains("1"));
        assertTrue(command.contains("/tmp/movie/123/thumbnail/uuid.jpg"));
    }

    private AppProperties properties() {
        return new AppProperties(
                new AppProperties.Worker("media.encode.requested", "worker-group", "ffmpeg", "ffprobe", "/tmp"),
<<<<<<< HEAD
                new AppProperties.Storage("ap-northeast-2"),
=======
                new AppProperties.Storage("s3", "ap-northeast-2", null),
>>>>>>> a4d4e61 (feat: 로컬에서 인코딩 테스트할 수 있는 환경 구성 및 문서 작업)
                new AppProperties.CoreApi(URI.create("http://localhost:8080"),
                        "/internal/api/media-jobs/{jobId}",
                        "/internal/api/movies/{movieId}/media",
                        "/internal/api/trailers/{jobId}/media",
                        "token")
        );
    }
}
