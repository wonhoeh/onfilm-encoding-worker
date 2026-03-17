# Consumer Development Spec

## Goal

프로듀서가 업로드 완료 후 Kafka 토픽 `media.encode.requested`로 발행한 메시지를 컨슈머가 consume해서 다음 작업을 끝까지 처리한다.

- `MediaEncodeJob` 상태 전이
- 원본 다운로드
- 인코딩 또는 썸네일 생성
- 결과물 업로드
- 실제 서비스 DB 미디어 경로 갱신
- 성공/실패 상태 반영

## Current Worker Scope

현재 워커는 다음 범위를 구현한다.

- Kafka 메시지 consume
- 메시지 검증
- `PROCESSING`, `DONE`, `FAILED` 상태 반영
- S3 원본 다운로드
- `ffmpeg` 기반 HLS 720p 인코딩
- `ffmpeg` 기반 1280x720 JPG 썸네일 생성
- S3 결과 업로드
- 코어 API 호출을 통한 movie/trailer/thumbnail 경로 갱신
- 작업 임시 디렉터리 정리

## Kafka Contract

### Topic

- topic: `media.encode.requested`
- message key: `jobId`

### Message Schema

```json
{
  "jobId": "uuid",
  "movieId": 123,
  "requestedByUserId": 45,
  "jobType": "MOVIE | TRAILER | THUMBNAIL",
  "preset": "VIDEO_HLS_720P_2500K_AAC_96K | THUMBNAIL_1280X720",
  "sourceBucket": "s3-bucket",
  "sourceKey": "movie/123/raw/file/uuid.mp4",
  "targetBucket": "s3-bucket",
  "targetKey": "movie/123/file/uuid/index.m3u8",
  "contentType": "video/mp4",
  "requestedAt": "2026-03-15T00:00:00Z"
}
```

### Enum Design

```java
public enum EncodeJobType {
    MOVIE,
    TRAILER,
    THUMBNAIL
}

public enum EncodeJobPreset {
    VIDEO_HLS_720P_2500K_AAC_96K,
    THUMBNAIL_1280X720
}
```

### Valid Mapping

- `MOVIE` + `VIDEO_HLS_720P_2500K_AAC_96K`
- `TRAILER` + `VIDEO_HLS_720P_2500K_AAC_96K`
- `THUMBNAIL` + `THUMBNAIL_1280X720`

기존 `MOVIE_720P_3000K`, `TRAILER_720P_3000K` preset은 사용하지 않는다.

## Encoding Policy

### Video

- container: HLS
- video codec: H.264
- audio codec: AAC
- resolution: `1280x720`
- video bitrate: `2500k`
- audio bitrate: `96k`
- audio channels: `2`
- audio sample rate: `48000`
- HLS segment duration: `6`
- segment format: `.ts`
- output manifest: `index.m3u8`

### Thumbnail

- image format: jpg
- resolution: `1280x720`

## Target Key Rules

비디오는 단일 mp4 파일이 아니라 HLS manifest 기준 경로를 사용한다.

### Movie

- manifest: `movie/{movieId}/file/{uuid}/index.m3u8`
- segment example: `movie/{movieId}/file/{uuid}/segment_000.ts`

### Trailer

- manifest: `movie/{movieId}/trailer/{uuid}/index.m3u8`
- segment example: `movie/{movieId}/trailer/{uuid}/segment_000.ts`

### Thumbnail

- image: `movie/{movieId}/thumbnail/{uuid}.jpg`

### Consumer Rule

- `targetKey`는 최종 결과물 기준 경로다.
- HLS segment는 `targetKey`의 부모 디렉터리에 함께 저장한다.
- 컨슈머는 `jobType`별 prefix/suffix를 검증한다.

## Processing Flow

컨슈머 처리 순서는 다음과 같다.

1. Kafka에서 메시지를 consume한다.
2. 메시지의 `jobType`, `preset`, `targetKey` 규칙을 검증한다.
3. `MediaEncodeJob.status = PROCESSING`, `startedAt`을 반영한다.
4. `sourceBucket/sourceKey`에서 원본 파일을 다운로드한다.
5. preset에 맞게 `ffmpeg`로 인코딩 또는 썸네일 생성을 수행한다.
6. 결과물을 `targetBucket/targetKey` 기준으로 업로드한다.
7. 성공 시 실제 서비스 DB의 미디어 경로를 갱신한다.
8. `MediaEncodeJob.status = DONE`, `completedAt`을 반영한다.
9. 실패 시 `MediaEncodeJob.status = FAILED`, `failureReason`, `completedAt`을 반영한다.
10. 작업 임시 디렉터리를 정리한다.

## Success Update Rules

인코딩 성공 후 컨슈머 또는 컨슈머가 호출하는 서버 서비스는 실제 미디어 엔티티를 갱신해야 한다.

- `MOVIE` 성공 시 movie의 `videoUrl`을 `targetKey`로 갱신
- `TRAILER` 성공 시 trailer의 `videoUrl`을 `targetKey`로 갱신
- `THUMBNAIL` 성공 시 movie의 `thumbnailUrl`을 `targetKey`로 갱신

## Worker Implementation Notes

### Core Components

- Kafka consumer: `src/main/java/kr/co/onfilm/encodingworker/interfaces/kafka/EncodingRequestedConsumer.java`
- processing orchestration: `src/main/java/kr/co/onfilm/encodingworker/application/EncodingJobProcessor.java`
- request validation: `src/main/java/kr/co/onfilm/encodingworker/application/EncodeRequestValidator.java`
- S3 integration: `src/main/java/kr/co/onfilm/encodingworker/infra/storage/S3StorageClient.java`
- ffmpeg command building: `src/main/java/kr/co/onfilm/encodingworker/infra/transcode/FfmpegCommandFactory.java`
- transcoding execution: `src/main/java/kr/co/onfilm/encodingworker/infra/transcode/FfmpegTranscoder.java`
- core API integration: `src/main/java/kr/co/onfilm/encodingworker/infra/coreapi/CoreApiClient.java`

### Core API Assumption

워커는 DB에 직접 연결하지 않고 코어 서버 내부 API를 호출하는 구조로 구현했다.

기본 설정값:

```yaml
app:
  core-api:
    base-url: http://localhost:8080
    media-jobs-path: /internal/api/media-jobs/{jobId}
    movies-path: /internal/api/movies/{movieId}/media
    trailers-path: /internal/api/trailers/{jobId}/media
    auth-token: change-me
```

의미:

- media job 상태 변경은 `PATCH /internal/api/media-jobs/{jobId}`
- movie 미디어 경로 갱신은 `PATCH /internal/api/movies/{movieId}/media`
- trailer 미디어 경로 갱신은 `PATCH /internal/api/trailers/{jobId}/media`

이 경로는 현재 워커 쪽 가정값이므로 서버 계약과 맞게 조정해야 한다.

## Runtime Requirements

- Java 17
- Kafka broker 연결 정보
- AWS 자격 증명
- S3 접근 권한
- `ffmpeg` 실행 파일
- 코어 API 인증 토큰

## Application Configuration

현재 기본 설정 예시는 다음과 같다.

```yaml
spring:
  kafka:
    consumer:
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer

app:
  worker:
    topic: media.encode.requested
    group-id: onfilm-encoding-worker
    ffmpeg-path: ffmpeg
    ffprobe-path: ffprobe
    working-dir: /tmp/onfilm-encoding-worker
  storage:
    region: ap-northeast-2
```

## Frontend Completion Condition

- 프론트는 `GET /api/media-jobs/{jobId}`를 polling 한다.
- 상태가 `DONE`이면 업로드 완료로 간주한다.
- 상태가 `FAILED`이면 실패 처리와 에러 메시지 노출을 수행한다.

## Validation and Test Status

구현 후 다음 테스트를 추가했다.

- `EncodeRequestValidatorTest`
- `FfmpegCommandFactoryTest`

검증 명령:

```bash
./gradlew test
```

## Open Items

- 코어 서버 내부 API 경로와 요청 body 계약 확정 필요
- `TRAILER` 엔티티를 `jobId`로 갱신하는 방식이 서버 계약과 맞는지 확인 필요
- 운영 환경에 `ffmpeg` 설치 및 PATH 또는 절대 경로 설정 필요
- Kafka bootstrap servers, AWS credentials 등 실제 배포 설정 주입 필요
