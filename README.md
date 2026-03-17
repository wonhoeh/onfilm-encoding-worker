# onfilm-encoding-worker

`onfilm-encoding-worker`는 Kafka에서 인코딩 요청 메시지를 소비한 뒤, 원본 파일을 S3에서 내려받아 `ffmpeg`로 변환하고 다시 S3에 업로드한 후 Core API에 결과를 반영하는 비동기 컨슈머입니다.

이 문서는 "컨슈머가 메시지를 받은 뒤 어떤 순서로 인코딩을 진행하는가"에 집중해서 설명합니다.

## 한눈에 보는 처리 순서

1. Kafka 토픽 `media.encode.requested`에서 인코딩 요청 메시지를 consume 한다.
2. 요청 본문과 `jobType`/`preset`/`targetKey` 조합이 유효한지 검증한다.
3. API 서버에 "이 작업을 지금부터 처리한다"는 상태를 보낸다.
4. 원본 미디어를 S3에서 로컬 작업 디렉터리로 다운로드한다.
5. `ffmpeg`를 실행해 HLS 영상 또는 썸네일 이미지를 생성한다.
6. 생성된 결과 파일들을 S3 target 경로로 업로드한다.
7. API 서버에 movie/trailer/thumbnail 결과 경로를 반영한다.
8. 작업 상태를 `DONE`으로 변경한다.
9. 실패하면 API 서버에 `FAILED` 상태를 남기고, 성공/실패와 관계없이 작업 디렉터리를 정리한다.

## 상태는 왜 따로 API 서버에 기록하나

이 Worker는 실제 인코딩을 수행하는 프로세스이고, 작업 상태 자체는 API 서버가 관리합니다.

즉 역할이 나뉘어 있습니다.

- Worker: Kafka 메시지를 받아 실제 인코딩 수행
- API 서버: 인코딩 작업 상태와 결과 경로를 저장

그래서 Worker는 단계가 바뀔 때마다 API 서버에 현재 상태를 알려줍니다.  
이렇게 해야 다른 서비스나 관리자 화면에서 "지금 이 작업이 대기 중인지, 인코딩 중인지, 완료됐는지, 실패했는지"를 확인할 수 있습니다.

상태 흐름은 보통 아래처럼 이해하면 됩니다.

```text
REQUESTED  ->  PROCESSING  ->  DONE
                  |
                  ->  FAILED
```

각 상태의 의미:

- `REQUESTED`: 인코딩 요청이 생성됨. 아직 Worker가 실제 작업을 시작하기 전
- `PROCESSING`: Worker가 요청을 받아 실제로 처리 중
- `DONE`: 인코딩, 업로드, 결과 경로 반영까지 모두 끝남
- `FAILED`: 처리 중간에 예외가 발생해서 완료하지 못함

## 실제 실행 흐름

아래 그림만 먼저 보면, 컨슈머가 메시지를 받은 뒤 어떤 순서로 움직이는지 빠르게 파악할 수 있습니다.

```mermaid
flowchart TD
    A[Kafka: media.encode.requested] --> B[Consumer가 메시지 수신
    EncodingRequestedConsumer.consume()]
    B --> C[요청 검증
    EncodeRequestValidator.validate()]
    C --> D[API 서버에 PROCESSING 상태 전송
    CoreApiClient.markProcessing()]
    D --> E[S3에서 원본 다운로드
    S3StorageClient.download()]
    E --> F[ffmpeg 인코딩
    FfmpegTranscoder.transcode()]
    F --> G[S3에 결과 업로드
    S3StorageClient.uploadFiles()]
    G --> H[API 서버에 결과 경로 반영
    CoreApiClient.updateMediaPath()]
    H --> I[API 서버에 DONE 상태 전송
    CoreApiClient.markDone()]
    I --> J[작업 디렉터리 정리
    EncodingJobProcessor.cleanup()]

    C --> X[API 서버에 FAILED 상태 전송
    CoreApiClient.markFailed()]
    E --> X
    F --> X
    G --> X
    H --> X
    X --> J
```

Mermaid를 지원하지 않는 환경에서는 아래 텍스트 도식으로 같은 흐름을 볼 수 있습니다.

```text
[Kafka: media.encode.requested]
              |
              v
[1. Consumer가 메시지 수신
 EncodingRequestedConsumer.consume()]
              |
              v
[2. 요청 검증
 EncodeRequestValidator.validate()]
              |
              v
[3. API 서버에 PROCESSING 상태 전송
 CoreApiClient.markProcessing()]
              |
              v
[4. S3에서 원본 다운로드
 S3StorageClient.download()]
              |
              v
[5. ffmpeg 인코딩
 FfmpegTranscoder.transcode()]
              |
              v
[6. S3에 결과 업로드
 S3StorageClient.uploadFiles()]
              |
              v
[7. API 서버에 결과 경로 반영
 CoreApiClient.updateMediaPath()]
              |
              v
[8. API 서버에 DONE 상태 전송
 CoreApiClient.markDone()]
              |
              v
[9. 작업 디렉터리 정리
 EncodingJobProcessor.cleanup()]

예외 발생 시:
[검증/다운로드/인코딩/업로드/경로 반영 중 실패]
              |
              v
[API 서버에 FAILED 상태 전송
 CoreApiClient.markFailed()]
              |
              v
[작업 디렉터리 정리
 EncodingJobProcessor.cleanup()]
```

### 1. Kafka Consumer가 메시지를 받는다

진입점은 `EncodingRequestedConsumer.consume()`입니다.

- 토픽: `app.worker.topic`
- 그룹 ID: `app.worker.group-id`
- 기본값: `media.encode.requested`, `onfilm-encoding-worker`

Consumer는 메시지를 받으면 바로 `EncodingJobProcessor.process()`로 위임합니다.

관련 코드:
- [EncodingRequestedConsumer.java](/Users/whheo/Desktop/onfilm/onfilm-encoding-worker/src/main/java/kr/co/onfilm/encodingworker/interfaces/kafka/EncodingRequestedConsumer.java)

### 2. 요청을 검증한다

`EncodingJobProcessor`는 가장 먼저 `EncodeRequestValidator.validate()`를 호출합니다.

여기서 확인하는 내용은 크게 두 가지입니다.

- `jobType`에 맞는 `preset`인지 확인
- `targetKey`가 기대하는 S3 경로 규칙을 만족하는지 확인

허용 조합:

| jobType | preset | targetKey 규칙 |
| --- | --- | --- |
| `MOVIE` | `VIDEO_HLS_720P_2500K_AAC_96K` | `movie/{movieId}/file/.../index.m3u8` |
| `TRAILER` | `VIDEO_HLS_720P_2500K_AAC_96K` | `movie/{movieId}/trailer/.../index.m3u8` |
| `THUMBNAIL` | `THUMBNAIL_1280X720` | `movie/{movieId}/thumbnail/...jpg` |

이 단계에서 검증에 실패하면 이후 인코딩은 진행되지 않고 실패 처리로 넘어갑니다.

관련 코드:
- [EncodeRequestValidator.java](/Users/whheo/Desktop/onfilm/onfilm-encoding-worker/src/main/java/kr/co/onfilm/encodingworker/application/EncodeRequestValidator.java)

### 3. API 서버에 "처리 시작" 상태를 보낸다

검증이 통과되면 `CoreApiClient.markProcessing()`이 호출됩니다.

이 호출은 "이제 이 job을 실제로 인코딩하기 시작한다"는 사실을 API 서버에 저장하는 단계입니다.

이때 API 서버의 media job 상태가 다음 값으로 갱신됩니다.

- `status = PROCESSING`
- `startedAt = 현재 시각`

즉 의미를 풀면 다음과 같습니다.

- Kafka에는 요청이 들어와 있었고
- Worker가 그 요청을 집어서
- 지금 실제 인코딩을 시작했다

왜 먼저 이 상태를 보내냐면, 인코딩은 시간이 걸리는 작업이기 때문입니다.  
API 서버에 `PROCESSING`이 기록되어 있어야 외부에서 "현재 작업 중"이라는 것을 알 수 있습니다.

관련 코드:
- [EncodingJobProcessor.java](/Users/whheo/Desktop/onfilm/onfilm-encoding-worker/src/main/java/kr/co/onfilm/encodingworker/application/EncodingJobProcessor.java)
- [CoreApiClient.java](/Users/whheo/Desktop/onfilm/onfilm-encoding-worker/src/main/java/kr/co/onfilm/encodingworker/infra/coreapi/CoreApiClient.java)

### 4. 원본 파일을 S3에서 다운로드한다

다음으로 `S3StorageClient.download()`가 실행됩니다.

- source bucket: 메시지의 `sourceBucket`
- source key: 메시지의 `sourceKey`
- 로컬 저장 위치: `{workingDir}/{jobId}/source/{원본파일명}`

기본 작업 디렉터리는 `/tmp/onfilm-encoding-worker`입니다.  
즉 각 작업은 `jobId`별 임시 디렉터리를 하나씩 사용합니다.

예시:

```text
/tmp/onfilm-encoding-worker/{jobId}/source/input.mp4
```

관련 코드:
- [S3StorageClient.java](/Users/whheo/Desktop/onfilm/onfilm-encoding-worker/src/main/java/kr/co/onfilm/encodingworker/infra/storage/S3StorageClient.java)
- [application.yml](/Users/whheo/Desktop/onfilm/onfilm-encoding-worker/src/main/resources/application.yml)

### 5. ffmpeg로 인코딩한다

원본 다운로드가 끝나면 `FfmpegTranscoder.transcode()`가 실행됩니다.

출력 파일 위치는 메시지의 `targetKey`를 그대로 로컬 작업 디렉터리 아래에 매핑합니다.

예를 들어 targetKey가 아래와 같다면:

```text
movie/10/file/hls/index.m3u8
```

로컬 출력 경로는 아래처럼 잡힙니다.

```text
/tmp/onfilm-encoding-worker/{jobId}/output/movie/10/file/hls/index.m3u8
```

현재 지원하는 인코딩 preset:

- `VIDEO_HLS_720P_2500K_AAC_96K`
  - 1280x720 스케일
  - HLS VOD 출력
  - 세그먼트 파일명: `segment_000.ts`, `segment_001.ts`, ...
- `THUMBNAIL_1280X720`
  - 영상에서 썸네일 1장 추출
  - 결과물은 `.jpg`

`ffmpeg` 프로세스가 0이 아닌 종료 코드를 반환하면 예외로 처리되고 전체 작업은 실패합니다.

관련 코드:
- [FfmpegTranscoder.java](/Users/whheo/Desktop/onfilm/onfilm-encoding-worker/src/main/java/kr/co/onfilm/encodingworker/infra/transcode/FfmpegTranscoder.java)
- [FfmpegCommandFactory.java](/Users/whheo/Desktop/onfilm/onfilm-encoding-worker/src/main/java/kr/co/onfilm/encodingworker/infra/transcode/FfmpegCommandFactory.java)

### 6. 결과 파일을 S3에 업로드한다

인코딩이 끝나면 `S3StorageClient.uploadFiles()`가 결과 파일 목록을 S3에 업로드합니다.

업로드 방식은 출력 유형에 따라 다음처럼 동작합니다.

- 썸네일인 경우: `.jpg` 한 파일만 업로드
- HLS인 경우: `index.m3u8`와 같은 디렉터리에 생성된 모든 파일을 업로드

예를 들어 HLS 출력이면 보통 다음 파일들이 업로드됩니다.

- `movie/{movieId}/file/.../index.m3u8`
- `movie/{movieId}/file/.../segment_000.ts`
- `movie/{movieId}/file/.../segment_001.ts`
- ...

콘텐츠 타입은 파일 확장자에 맞게 설정됩니다.

관련 코드:
- [S3StorageClient.java](/Users/whheo/Desktop/onfilm/onfilm-encoding-worker/src/main/java/kr/co/onfilm/encodingworker/infra/storage/S3StorageClient.java)

### 7. API 서버에 결과 경로를 반영한다

S3 업로드 후에는 `CoreApiClient.updateMediaPath()`가 호출됩니다.

`jobType`에 따라 반영 대상이 달라집니다.

- `MOVIE`: movie의 `videoUrl` 갱신
- `THUMBNAIL`: movie의 `thumbnailUrl` 갱신
- `TRAILER`: trailer의 `videoUrl` 갱신

여기서 저장되는 경로 값은 메시지의 `targetKey`입니다.

즉 Worker는 "실제 파일 업로드"만 하는 것이 아니라, API 서버에도 "이 파일을 이제 여기 경로로 보면 된다"는 정보까지 같이 반영합니다.

관련 코드:
- [CoreApiClient.java](/Users/whheo/Desktop/onfilm/onfilm-encoding-worker/src/main/java/kr/co/onfilm/encodingworker/infra/coreapi/CoreApiClient.java)

### 8. API 서버에 완료 상태를 보낸다

모든 단계가 정상적으로 끝나면 `CoreApiClient.markDone()`이 호출됩니다.

- `status = DONE`
- `completedAt = 현재 시각`

이 상태까지 기록되면 아래 의미가 모두 충족됩니다.

- 인코딩이 끝났고
- 결과 파일이 S3에 올라갔고
- API 서버에도 결과 경로가 반영됐고
- 해당 job이 정상 완료로 마감됐다

여기까지 오면 하나의 인코딩 job이 정상 완료된 것입니다.

## 실패 시 흐름

처리 중 어느 단계에서든 예외가 발생하면 아래 순서로 동작합니다.

1. 에러 로그를 남긴다.
2. 예외 메시지를 요약해 `failureReason`을 만든다.
3. Core API에 `status = FAILED`로 반영한다.
4. `completedAt`을 기록한다.
5. 마지막으로 작업 디렉터리를 정리한다.

즉 실패하더라도 "어디까지 하다가 실패했는지"를 외부에서 알 수 있도록 API 서버에 실패 상태를 남기려고 시도합니다.

실제 상태 흐름을 예시로 보면:

- 정상 완료: `REQUESTED -> PROCESSING -> DONE`
- 중간 실패: `REQUESTED -> PROCESSING -> FAILED`

## 작업 디렉터리 정리

`EncodingJobProcessor`는 `finally` 블록에서 항상 작업 디렉터리를 정리합니다.

정리 대상:

- `{workingDir}/{jobId}/source`
- `{workingDir}/{jobId}/output`
- 그 하위에 생성된 모든 파일

따라서 이 Worker는 인코딩 중간 산출물을 로컬에 영구 보관하지 않습니다.

## 메시지 예시

Kafka 메시지는 `MediaEncodeRequestedMessage` 형태로 역직렬화됩니다.

```json
{
  "jobId": "11111111-2222-3333-4444-555555555555",
  "movieId": 10,
  "requestedByUserId": 7,
  "jobType": "MOVIE",
  "preset": "VIDEO_HLS_720P_2500K_AAC_96K",
  "sourceBucket": "onfilm-source",
  "sourceKey": "raw/movie-10/input.mp4",
  "targetBucket": "onfilm-media",
  "targetKey": "movie/10/file/hls/index.m3u8",
  "contentType": "video/mp4",
  "requestedAt": "2026-03-15T10:00:00Z"
}
```

필수 필드:

- `jobId`
- `movieId`
- `requestedByUserId`
- `jobType`
- `preset`
- `sourceBucket`
- `sourceKey`
- `targetBucket`
- `targetKey`
- `contentType`
- `requestedAt`

관련 코드:
- [MediaEncodeRequestedMessage.java](/Users/whheo/Desktop/onfilm/onfilm-encoding-worker/src/main/java/kr/co/onfilm/encodingworker/domain/MediaEncodeRequestedMessage.java)

## 설정값

주요 설정은 `src/main/resources/application.yml`에 있습니다.

```yaml
app:
  worker:
    topic: media.encode.requested
    group-id: onfilm-encoding-worker
    ffmpeg-path: ffmpeg
    ffprobe-path: ffprobe
    working-dir: /tmp/onfilm-encoding-worker
```

함께 확인할 값:

- Kafka bootstrap server
- S3 region
- Core API base URL
- Core API 인증 토큰

관련 코드:
- [application.yml](/Users/whheo/Desktop/onfilm/onfilm-encoding-worker/src/main/resources/application.yml)
- [AppProperties.java](/Users/whheo/Desktop/onfilm/onfilm-encoding-worker/src/main/java/kr/co/onfilm/encodingworker/config/AppProperties.java)

## 로컬 실행

사전 조건:

- Java 17
- `ffmpeg` 실행 가능
- Kafka 접근 가능
- S3 접근 가능한 AWS 인증정보 설정
- Core API 접근 가능

실행:

```bash
./gradlew bootRun
```

테스트:

```bash
./gradlew test
```

## 코드 기준으로 먼저 읽어야 할 파일

컨슈머의 처리 순서를 빠르게 파악하려면 아래 순서로 읽는 것이 가장 쉽습니다.

1. [EncodingRequestedConsumer.java](/Users/whheo/Desktop/onfilm/onfilm-encoding-worker/src/main/java/kr/co/onfilm/encodingworker/interfaces/kafka/EncodingRequestedConsumer.java)
2. [EncodingJobProcessor.java](/Users/whheo/Desktop/onfilm/onfilm-encoding-worker/src/main/java/kr/co/onfilm/encodingworker/application/EncodingJobProcessor.java)
3. [EncodeRequestValidator.java](/Users/whheo/Desktop/onfilm/onfilm-encoding-worker/src/main/java/kr/co/onfilm/encodingworker/application/EncodeRequestValidator.java)
4. [S3StorageClient.java](/Users/whheo/Desktop/onfilm/onfilm-encoding-worker/src/main/java/kr/co/onfilm/encodingworker/infra/storage/S3StorageClient.java)
5. [FfmpegTranscoder.java](/Users/whheo/Desktop/onfilm/onfilm-encoding-worker/src/main/java/kr/co/onfilm/encodingworker/infra/transcode/FfmpegTranscoder.java)
6. [FfmpegCommandFactory.java](/Users/whheo/Desktop/onfilm/onfilm-encoding-worker/src/main/java/kr/co/onfilm/encodingworker/infra/transcode/FfmpegCommandFactory.java)
7. [CoreApiClient.java](/Users/whheo/Desktop/onfilm/onfilm-encoding-worker/src/main/java/kr/co/onfilm/encodingworker/infra/coreapi/CoreApiClient.java)
