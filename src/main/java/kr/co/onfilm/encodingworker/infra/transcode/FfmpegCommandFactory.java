package kr.co.onfilm.encodingworker.infra.transcode;

import java.nio.file.Path;
import java.util.List;
import kr.co.onfilm.encodingworker.config.AppProperties;
import kr.co.onfilm.encodingworker.domain.EncodeJobPreset;
import org.springframework.stereotype.Component;

@Component
public class FfmpegCommandFactory {

    private final AppProperties properties;

    public FfmpegCommandFactory(AppProperties properties) {
        this.properties = properties;
    }

    public List<String> create(EncodeJobPreset preset, Path source, Path target) {
        return switch (preset) {
            case VIDEO_HLS_720P_2500K_AAC_96K -> List.of(
                    properties.worker().ffmpegPath(),
                    "-y",
                    "-i", source.toString(),
                    "-vf", "scale=w=1280:h=720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2",
                    "-c:v", "libx264",
                    "-profile:v", "high",
                    "-preset", "medium",
                    "-b:v", "2500k",
                    "-maxrate", "2675k",
                    "-bufsize", "3750k",
                    "-pix_fmt", "yuv420p",
                    "-c:a", "aac",
                    "-b:a", "96k",
                    "-ac", "2",
                    "-ar", "48000",
                    "-f", "hls",
                    "-hls_time", "6",
                    "-hls_playlist_type", "vod",
                    "-hls_segment_type", "mpegts",
                    "-hls_segment_filename", target.getParent().resolve("segment_%03d.ts").toString(),
                    target.toString()
            );
            case THUMBNAIL_1280X720 -> List.of(
                    properties.worker().ffmpegPath(),
                    "-y",
                    "-i", source.toString(),
                    "-vf", "thumbnail,scale=w=1280:h=720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2",
                    "-frames:v", "1",
                    target.toString()
            );
        };
    }
}
