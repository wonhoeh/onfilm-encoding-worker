# API Server Media Job Callback Spec

워커는 DB의 미디어 엔티티를 직접 변경하지 않고 Core API의 job 단위 내부 API를 호출합니다. 서버는 콜백을 멱등하게 처리해야 합니다. 네트워크 응답을 받기 전에 연결이 끊기면 워커가 같은 콜백을 다시 보낼 수 있습니다.

## Endpoints

모든 성공 응답은 `204 No Content`를 권장합니다.

### Processing

```http
POST /internal/api/media-jobs/{jobId}/processing
Content-Type: application/json

{"startedAt":"2026-08-21T00:00:00Z"}
```

### Complete

완료 API가 job 상태와 실제 Movie/Trailer/Thumbnail 경로를 하나의 서버 트랜잭션에서 함께 반영합니다.

```http
POST /internal/api/media-jobs/{jobId}/complete
Content-Type: application/json

{
  "outputBucket":"onfilm-media",
  "outputKey":"movie/123/file/99999999-8888-7777-6666-555555555555/index.m3u8",
  "contentType":"application/vnd.apple.mpegurl",
  "completedAt":"2026-08-21T00:03:00Z"
}
```

### Fail

```http
POST /internal/api/media-jobs/{jobId}/fail
Content-Type: application/json

{
  "failureCode":"ENCODE_FAILED",
  "failureReason":"ffmpeg exited with code 1",
  "completedAt":"2026-08-21T00:03:00Z"
}
```

failure code:

- `UNSUPPORTED_MESSAGE_SCHEMA`
- `INVALID_REQUEST`
- `SOURCE_NOT_FOUND`
- `SOURCE_DOWNLOAD_FAILED`
- `UNSUPPORTED_MEDIA`
- `ENCODE_TIMEOUT`
- `ENCODE_FAILED`
- `OUTPUT_VALIDATION_FAILED`
- `OUTPUT_UPLOAD_FAILED`
- `CORE_API_UNAVAILABLE`
- `UNEXPECTED_WORKER_ERROR`

`failureReason`은 최대 1,000자로 제한하고 알려진 credential 표현을 마스킹합니다. 원본 토큰이나 HMAC secret을 포함하면 안 됩니다.

## HMAC authentication

각 요청은 다음 헤더를 포함합니다.

```text
X-Onfilm-Timestamp: epoch seconds
X-Onfilm-Nonce: UUID
X-Onfilm-Signature: lowercase hex HMAC-SHA256
```

서명 원문:

```text
{timestamp}\n{nonce}\n{HTTP_METHOD}\n{raw_path}\n{lowercase_hex_sha256_of_raw_body}
```

`MEDIA_ENCODE_CALLBACK_SECRET`을 HMAC-SHA256 key로 사용합니다. query string이 없는 현재 계약에서는 raw path만 서명합니다. 서버는 일정 시간 범위 밖의 timestamp와 이미 소비한 nonce를 거부해야 하며, 서명 비교는 constant-time 비교를 사용해야 합니다.

## Status transitions and idempotency

정상 흐름:

```text
REQUESTED -> PROCESSING -> DONE
                      \-> FAILED
```

권장 규칙:

- 동일 상태/동일 결과 재요청은 성공으로 응답한다.
- 이미 `DONE`인 job의 processing/fail 요청은 상태를 되돌리지 않는다.
- 이미 `FAILED`인 job의 processing/complete 요청은 상태를 되돌리지 않는다.
- 존재하지 않는 `jobId`는 `404`, 계약 위반은 `400` 또는 `409`로 응답한다.
- `408`, `429`, `5xx`는 워커가 재시도한다.
- 다른 일반 `4xx`는 영구 실패로 취급한다.

워커가 `OUTPUT_UPLOADED`를 저장한 뒤 complete 응답 전에 종료될 수 있으므로, complete는 반드시 안전하게 반복 호출할 수 있어야 합니다.
