package kr.co.onfilm.encodingworker.infra.coreapi;

import kr.co.onfilm.encodingworker.application.FailureCode;
import kr.co.onfilm.encodingworker.config.AppProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.*;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class CoreApiClient {
    private final RestClient restClient;
    private final AppProperties properties;

    public void markProcessing(UUID jobId, Instant startedAt) {
        post(properties.coreApi().processingPath(), jobId,
                Map.of("startedAt", startedAt.toString()), "mark processing");
    }

    public void complete(UUID jobId, String outputBucket, String outputKey,
                         String contentType, Instant completedAt) {
        post(properties.coreApi().completionPath(), jobId, Map.of(
                "outputBucket", outputBucket,
                "outputKey", outputKey,
                "contentType", contentType,
                "completedAt", completedAt.toString()
        ), "complete");
    }

    public void markFailed(UUID jobId, FailureCode failureCode,
                           String failureReason, Instant completedAt) {
        post(properties.coreApi().failurePath(), jobId, Map.of(
                "failureCode", failureCode.name(),
                "failureReason", sanitizeReason(failureReason),
                "completedAt", completedAt.toString()
        ), "mark failed");
    }

    private void post(String path, UUID jobId, Map<String, Object> body, String operation) {
        try {
            restClient.post()
                    .uri(path, jobId)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .toBodilessEntity();
        } catch (HttpClientErrorException exception) {
            throw new CoreApiException(
                    "Core API rejected request to " + operation + " job " + jobId,
                    exception.getStatusCode().is5xxServerError()
                            || exception.getStatusCode().value() == 408
                            || exception.getStatusCode().value() == 429,
                    exception);
        } catch (RestClientException exception) {
            throw new CoreApiException(
                    "Core API unavailable while trying to " + operation + " job " + jobId,
                    true,
                    exception);
        }
    }

    public static String sanitizeReason(String reason) {
        String resolved = reason == null || reason.isBlank() ? "Unknown worker failure" : reason.trim();
        resolved = resolved.replaceAll("(?i)(authorization|token|secret|password)=[^\\s,]+", "$1=[REDACTED]");
        return resolved.length() <= 1000 ? resolved : resolved.substring(0, 1000);
    }
}
