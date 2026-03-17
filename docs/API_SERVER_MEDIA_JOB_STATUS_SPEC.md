# API Server Media Job Status Spec

## Goal

인코딩 워커가 Kafka 메시지를 처리하면서 바뀌는 작업 상태를 API 서버가 저장할 수 있어야 한다.

이 문서는 워커가 현재 어떤 시점에 어떤 요청을 보내는지 기준으로, API 서버가 구현해야 하는 상태 저장 계약을 정리한다.

## Why This Is Needed

워커는 실제 인코딩을 수행하는 프로세스이고, 작업 상태와 결과 경로는 API 서버가 관리해야 한다.

즉 역할은 다음처럼 나뉜다.

- Worker: Kafka consume, 다운로드, 인코딩, 업로드 수행
- API Server: job 상태 저장, 결과 경로 저장, 외부 조회 제공

현재 워커는 DB에 직접 접근하지 않고 API 서버 내부 API만 호출한다.

따라서 API 서버에는 최소한 아래 기능이 필요하다.

- `MediaJob` 상태 변경 저장
- 실패 사유 저장
- 시작 시각/완료 시각 저장
- 성공 시 movie/trailer/thumbnail 경로 반영

## Worker Call Timing

워커는 아래 순서로 API 서버를 호출한다.

1. 요청 검증 통과
2. `PATCH /internal/api/media-jobs/{jobId}` with `PROCESSING`
3. 인코딩 수행
4. 성공 시 결과 경로 갱신 API 호출
5. 성공 시 `PATCH /internal/api/media-jobs/{jobId}` with `DONE`
6. 실패 시 `PATCH /internal/api/media-jobs/{jobId}` with `FAILED`

관련 워커 코드:

- [EncodingJobProcessor.java](/Users/whheo/Desktop/onfilm/onfilm-encoding-worker/src/main/java/kr/co/onfilm/encodingworker/application/EncodingJobProcessor.java#L29)
- [CoreApiClient.java](/Users/whheo/Desktop/onfilm/onfilm-encoding-worker/src/main/java/kr/co/onfilm/encodingworker/infra/coreapi/CoreApiClient.java#L22)

## Required Internal APIs

### 1. Media Job Status Update

- method: `PATCH`
- path: `/internal/api/media-jobs/{jobId}`
- purpose: 인코딩 작업 상태 변경 저장

#### Path Variable

- `jobId: UUID`

#### Request Body

아래 필드 조합 중 하나를 받을 수 있어야 한다.

`PROCESSING` 전환:

```json
{
  "status": "PROCESSING",
  "startedAt": "2026-03-15T10:00:00Z"
}
```

`DONE` 전환:

```json
{
  "status": "DONE",
  "completedAt": "2026-03-15T10:03:00Z"
}
```

`FAILED` 전환:

```json
{
  "status": "FAILED",
  "failureReason": "ffmpeg exited with code 1",
  "completedAt": "2026-03-15T10:03:00Z"
}
```

#### Accepted Status Values

- `REQUESTED`
- `PROCESSING`
- `DONE`
- `FAILED`

실제 워커가 현재 보내는 값은 아래 세 가지다.

- `PROCESSING`
- `DONE`
- `FAILED`

`REQUESTED`는 보통 job 생성 시점에 서버에서 먼저 저장되어 있어야 한다.

#### Field Rules

- `status`
  - 필수
  - enum: `REQUESTED | PROCESSING | DONE | FAILED`
- `startedAt`
  - `PROCESSING`일 때 허용
  - 서버 저장 대상
- `completedAt`
  - `DONE` 또는 `FAILED`일 때 허용
  - 서버 저장 대상
- `failureReason`
  - `FAILED`일 때 허용
  - 서버 저장 대상

#### Recommended Server Validation

- `jobId`에 해당하는 job이 없으면 `404`
- `PROCESSING`인데 `startedAt`이 없으면 `400`
- `DONE`인데 `completedAt`이 없으면 `400`
- `FAILED`인데 `completedAt`이 없으면 `400`
- `FAILED`인데 `failureReason`이 비어 있으면 `400`
- 이미 완료된 job에 대한 비정상 상태 역전이 필요하면 정책을 정해서 막거나 무시

#### Recommended Persistence Rules

- `status = PROCESSING`
  - `status`를 `PROCESSING`으로 변경
  - `startedAt` 저장
- `status = DONE`
  - `status`를 `DONE`으로 변경
  - `completedAt` 저장
- `status = FAILED`
  - `status`를 `FAILED`로 변경
  - `failureReason` 저장
  - `completedAt` 저장

#### Recommended Response

- `204 No Content`

### 2. Movie Media Path Update

- method: `PATCH`
- path: `/internal/api/movies/{movieId}/media`
- purpose: movie video 또는 thumbnail 경로 반영

#### Request Body Cases

movie video:

```json
{
  "videoUrl": "movie/10/file/hls/index.m3u8"
}
```

movie thumbnail:

```json
{
  "thumbnailUrl": "movie/10/thumbnail/thumb.jpg"
}
```

#### Recommended Response

- `204 No Content`

### 3. Trailer Media Path Update

- method: `PATCH`
- path: `/internal/api/trailers/{jobId}/media`
- purpose: trailer video 경로 반영

#### Request Body

```json
{
  "movieId": 10,
  "videoUrl": "movie/10/trailer/hls/index.m3u8"
}
```

#### Recommended Response

- `204 No Content`

## State Transition Rules

API 서버는 최소한 아래 상태 흐름을 처리할 수 있어야 한다.

정상 완료:

```text
REQUESTED -> PROCESSING -> DONE
```

실패:

```text
REQUESTED -> PROCESSING -> FAILED
```

각 상태 의미:

- `REQUESTED`: job 생성됨, 아직 워커가 시작하지 않음
- `PROCESSING`: 워커가 실제 인코딩 작업 시작
- `DONE`: 인코딩, 업로드, 결과 경로 반영까지 완료
- `FAILED`: 처리 중 예외 발생

## Minimal Data Model Expectation

API 서버 DB에는 최소한 아래 필드를 저장할 수 있어야 한다.

- `job_id`
- `status`
- `started_at`
- `completed_at`
- `failure_reason`
- `movie_id`
- `job_type`
- `target_key`

정확한 테이블 구조는 서버 구현에 맞게 다를 수 있지만, 워커 계약상 위 상태 값들은 저장 가능해야 한다.

## Example End-to-End Sequence

예를 들어 `jobId = 11111111-2222-3333-4444-555555555555` 인 작업이 정상 완료되면:

1. 서버는 job 생성 시 `REQUESTED` 상태를 저장한다.
2. 워커가 작업 시작 직후 아래 요청을 보낸다.

```http
PATCH /internal/api/media-jobs/11111111-2222-3333-4444-555555555555
Content-Type: application/json

{
  "status": "PROCESSING",
  "startedAt": "2026-03-15T10:00:00Z"
}
```

3. 인코딩 및 업로드가 끝나면 결과 경로 반영 요청을 받는다.
4. 마지막으로 아래 요청을 받는다.

```http
PATCH /internal/api/media-jobs/11111111-2222-3333-4444-555555555555
Content-Type: application/json

{
  "status": "DONE",
  "completedAt": "2026-03-15T10:03:00Z"
}
```

실패하면 마지막 요청은 아래처럼 바뀐다.

```http
PATCH /internal/api/media-jobs/11111111-2222-3333-4444-555555555555
Content-Type: application/json

{
  "status": "FAILED",
  "failureReason": "ffmpeg exited with code 1",
  "completedAt": "2026-03-15T10:03:00Z"
}
```

## Current Worker Request Contract

현재 워커 구현 기준으로 API 서버는 아래 요청 body를 그대로 받아들일 수 있어야 한다.

`markProcessing()`:

```json
{
  "status": "PROCESSING",
  "startedAt": "ISO-8601 instant"
}
```

`markDone()`:

```json
{
  "status": "DONE",
  "completedAt": "ISO-8601 instant"
}
```

`markFailed()`:

```json
{
  "status": "FAILED",
  "failureReason": "string",
  "completedAt": "ISO-8601 instant"
}
```

관련 코드:

- [CoreApiClient.java](/Users/whheo/Desktop/onfilm/onfilm-encoding-worker/src/main/java/kr/co/onfilm/encodingworker/infra/coreapi/CoreApiClient.java#L22)

## Open Items

- `PATCH /internal/api/media-jobs/{jobId}` 응답 body를 둘지 `204`만 반환할지 최종 확정 필요
- 완료된 job에 대한 중복 `DONE` 또는 `FAILED` 요청을 어떻게 처리할지 정책 확정 필요
- `TRAILER` 경로 갱신 API가 `jobId` 기준이 맞는지 서버와 재확인 필요
- 외부 조회 API에서 상태를 어떤 DTO로 노출할지 별도 확정 필요
