package kr.co.onfilm.encodingworker.application;

import kr.co.onfilm.encodingworker.domain.EncodeJobPreset;
import kr.co.onfilm.encodingworker.domain.EncodeJobType;
import kr.co.onfilm.encodingworker.domain.MediaEncodeRequestedMessage;
import org.springframework.stereotype.Component;

@Component
public class EncodeRequestValidator {

    public void validate(MediaEncodeRequestedMessage message) {
        boolean validPreset = switch (message.jobType()) {
            case MOVIE, TRAILER -> message.preset() == EncodeJobPreset.VIDEO_HLS_720P_2500K_AAC_96K;
            case THUMBNAIL -> message.preset() == EncodeJobPreset.THUMBNAIL_1280X720;
        };
        if (!validPreset) {
            throw new IllegalArgumentException("Invalid preset for job type: " + message.jobType() + "/" + message.preset());
        }

        switch (message.jobType()) {
            case MOVIE -> requireTarget(message.targetKey(), "movie/%d/file/".formatted(message.movieId()), "/index.m3u8");
            case TRAILER -> requireTarget(message.targetKey(), "movie/%d/trailer/".formatted(message.movieId()), "/index.m3u8");
            case THUMBNAIL -> requireTarget(message.targetKey(), "movie/%d/thumbnail/".formatted(message.movieId()), ".jpg");
        }
    }

    private void requireTarget(String targetKey, String prefix, String suffix) {
        if (!targetKey.startsWith(prefix) || !targetKey.endsWith(suffix)) {
            throw new IllegalArgumentException("Invalid targetKey: " + targetKey);
        }
    }
}
