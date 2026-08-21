package kr.co.onfilm.encodingworker.application;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import kr.co.onfilm.encodingworker.TestProperties;
import kr.co.onfilm.encodingworker.domain.*;
import kr.co.onfilm.encodingworker.infra.coreapi.CoreApiClient;
import kr.co.onfilm.encodingworker.infra.storage.*;
import kr.co.onfilm.encodingworker.infra.transcode.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.*;
import java.time.*;
import java.util.*;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EncodingJobProcessorTest {
    @Mock EncodeRequestValidator validator;
    @Mock InboxClaimCoordinator claimCoordinator;
    @Mock InboxTransactionService inbox;
    @Mock CoreApiClient coreApi;
    @Mock StorageClient storage;
    @Mock MediaProbe probe;
    @Mock FfmpegTranscoder transcoder;
    @Mock EncodedOutputValidator outputValidator;
    @TempDir Path tempDir;
    private final Instant now = Instant.parse("2026-08-21T00:00:00Z");
    private EncodingJobProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new EncodingJobProcessor(
                TestProperties.create(tempDir.toString()), validator, claimCoordinator, inbox,
                coreApi, storage, probe, transcoder, outputValidator,
                Clock.fixed(now, ZoneOffset.UTC), new SimpleMeterRegistry());
    }

    @Test
    void callbackOnlyDoesNotDownloadOrTranscodeAgain() {
        MediaEncodeRequestedMessage message = message();
        given(claimCoordinator.claim(message.jobId().toString(), message))
                .willReturn(InboxClaim.CALLBACK_ONLY);

        processor.process(message.jobId().toString(), message);

        verify(coreApi).complete(message.jobId(), message.targetBucket(), message.targetKey(),
                message.targetContentType(), now);
        verify(inbox).markDone(message.jobId());
        verifyNoInteractions(storage, transcoder, probe, validator);
    }

    @Test
    void successStoresOutputUploadedBeforeCompletionCallback() {
        MediaEncodeRequestedMessage message = message();
        Path source = tempDir.resolve("source.mp4");
        Path manifest = tempDir.resolve("index.m3u8");
        EncodedOutput output = new EncodedOutput(message.targetContentType(), List.of(manifest));
        given(claimCoordinator.claim(message.jobId().toString(), message)).willReturn(InboxClaim.PROCESS);
        given(storage.metadata(message.sourceBucket(), message.sourceKey()))
                .willReturn(new StorageObjectMetadata(100, message.sourceContentType()));
        given(storage.download(eq(message.sourceBucket()), eq(message.sourceKey()), any())).willReturn(source);
        given(transcoder.transcode(eq(message), eq(source), any())).willReturn(output);

        processor.process(message.jobId().toString(), message);

        InOrder order = inOrder(storage, inbox, coreApi);
        order.verify(storage).uploadFiles(
                message.targetBucket(), message.targetKey(), output.files(), output.contentType());
        order.verify(inbox).markOutputUploaded(message.jobId());
        order.verify(coreApi).complete(
                message.jobId(), message.targetBucket(), message.targetKey(), message.targetContentType(), now);
        order.verify(inbox).markDone(message.jobId());
    }

    @Test
    void transientStorageFailureIsRetriedWithoutFailedCallback() {
        MediaEncodeRequestedMessage message = message();
        given(claimCoordinator.claim(message.jobId().toString(), message)).willReturn(InboxClaim.PROCESS);
        given(storage.metadata(message.sourceBucket(), message.sourceKey()))
                .willThrow(new StorageException("temporary s3 failure", true, null));

        assertThatThrownBy(() -> processor.process(message.jobId().toString(), message))
                .isInstanceOf(RetryableEncodingException.class);
        verify(inbox).recordFailure(
                eq(message.jobId()), eq(FailureCode.SOURCE_DOWNLOAD_FAILED),
                contains("temporary s3 failure"), eq(true));
        verify(coreApi, never()).markFailed(any(), any(), any(), any());
    }

    @Test
    void permanentValidationFailureWaitsForDltFailureReporter() {
        MediaEncodeRequestedMessage message = message();
        given(claimCoordinator.claim(message.jobId().toString(), message)).willReturn(InboxClaim.PROCESS);
        doThrow(new PermanentEncodingException(FailureCode.INVALID_REQUEST, "bad request"))
                .when(validator).validate(message.jobId().toString(), message);

        assertThatThrownBy(() -> processor.process(message.jobId().toString(), message))
                .isInstanceOf(PermanentEncodingException.class);
        verify(inbox).recordFailure(
                message.jobId(), FailureCode.INVALID_REQUEST, "PermanentEncodingException: bad request", false);
        verify(coreApi, never()).markFailed(any(), any(), any(), any());
    }

    private MediaEncodeRequestedMessage message() {
        UUID jobId = UUID.randomUUID();
        UUID requestId = UUID.randomUUID();
        return new MediaEncodeRequestedMessage(
                1, jobId, requestId, 1L, 2L,
                EncodeJobType.MOVIE, EncodeJobPreset.VIDEO_HLS_720P_2500K_AAC_96K,
                "bucket", "movie/1/raw/file/" + requestId + ".mp4",
                "bucket", "movie/1/file/" + jobId + "/index.m3u8",
                "video/mp4", "application/vnd.apple.mpegurl", now.minusSeconds(10));
    }
}
