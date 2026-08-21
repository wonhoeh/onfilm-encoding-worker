package kr.co.onfilm.encodingworker;

import kr.co.onfilm.encodingworker.config.AppProperties;

import java.net.URI;
import java.time.Duration;
import java.util.Set;

public final class TestProperties {
    public static final String SECRET = "test-media-encode-callback-secret-32-bytes";

    private TestProperties() {
    }

    public static AppProperties create() {
        return create("/tmp");
    }

    public static AppProperties create(String workingDir) {
        return new AppProperties(
                new AppProperties.Worker(
                        "media.encode.requested", "worker-group", "ffmpeg", "ffprobe", workingDir,
                        Duration.ofHours(2), Duration.ofHours(3), 10_737_418_240L,
                        Duration.ofHours(6), 1, 5, 1000, 2.0, 60_000, 60_000),
                new AppProperties.Storage(
                        "s3", "ap-northeast-2", null, Set.of("bucket"),
                        Duration.ofMinutes(10), Duration.ofMinutes(3)),
                new AppProperties.CoreApi(
                        URI.create("http://localhost:8080"),
                        "/internal/api/media-jobs/{jobId}/processing",
                        "/internal/api/media-jobs/{jobId}/complete",
                        "/internal/api/media-jobs/{jobId}/fail",
                        SECRET, Duration.ofSeconds(5), Duration.ofSeconds(30))
        );
    }
}
