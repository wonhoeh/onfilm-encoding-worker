package kr.co.onfilm.encodingworker.infra.coreapi;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import kr.co.onfilm.encodingworker.config.AppProperties;
import kr.co.onfilm.encodingworker.domain.EncodeJobType;
import kr.co.onfilm.encodingworker.domain.MediaJobStatus;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

@Component
@RequiredArgsConstructor
public class CoreApiClient {

    private final RestClient restClient;
    private final AppProperties properties;

    public void markProcessing(UUID jobId, Instant startedAt) {
        patchJob(jobId, Map.of(
                "status", MediaJobStatus.PROCESSING.name(),
                "startedAt", startedAt.toString()
        ));
    }

    public void markDone(UUID jobId, Instant completedAt) {
        patchJob(jobId, Map.of(
                "status", MediaJobStatus.DONE.name(),
                "completedAt", completedAt.toString()
        ));
    }

    public void markFailed(UUID jobId, String failureReason, Instant completedAt) {
        patchJob(jobId, Map.of(
                "status", MediaJobStatus.FAILED.name(),
                "failureReason", failureReason,
                "completedAt", completedAt.toString()
        ));
    }

    public void updateMediaPath(EncodeJobType jobType, Long movieId, UUID jobId, String targetKey) {
        try {
            switch (jobType) {
                case MOVIE -> restClient.patch()
                        .uri(properties.coreApi().moviesPath(), movieId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(Map.of("videoUrl", targetKey))
                        .retrieve()
                        .toBodilessEntity();
                case THUMBNAIL -> restClient.patch()
                        .uri(properties.coreApi().moviesPath(), movieId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(Map.of("thumbnailUrl", targetKey))
                        .retrieve()
                        .toBodilessEntity();
                case TRAILER -> restClient.patch()
                        .uri(properties.coreApi().trailersPath(), jobId)
                        .contentType(MediaType.APPLICATION_JSON)
<<<<<<< HEAD
                        .body(Map.of("movieId", movieId, "videoUrl", targetKey))
=======
                        .body(Map.of("trailerUrl", targetKey))
>>>>>>> a4d4e61 (feat: 로컬에서 인코딩 테스트할 수 있는 환경 구성 및 문서 작업)
                        .retrieve()
                        .toBodilessEntity();
            }
        } catch (RestClientException exception) {
            throw new CoreApiException("Failed to update media path for job " + jobId, exception);
        }
    }

    private void patchJob(UUID jobId, Map<String, Object> body) {
        try {
            restClient.patch()
                    .uri(properties.coreApi().mediaJobsPath(), jobId)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .toBodilessEntity();
        } catch (RestClientException exception) {
            throw new CoreApiException("Failed to update media job " + jobId, exception);
        }
    }
}
