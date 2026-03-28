package kr.co.onfilm.encodingworker.application;

import java.util.UUID;

public class JobAlreadyProcessingException extends RuntimeException {

    public JobAlreadyProcessingException(UUID jobId) {
        super("Job is already being processed or completed: " + jobId);
    }
}
