# Consumer Development Spec

현재 구현의 메시지 형식, 경로 규칙, 실행 방법은 [README](../README.md)를 기준으로 합니다. Core API 콜백은 [API Server Media Job Callback Spec](API_SERVER_MEDIA_JOB_STATUS_SPEC.md), 중복 처리·복구·서명 정책은 [Worker reliability decisions](WORKER_RELIABILITY_DECISIONS.md)를 따릅니다.

## Component boundaries

- `EncodingRequestedConsumer`: Kafka retry topic 및 DLT 경계
- `EncodingJobProcessor`: 처리 단계 orchestration과 오류 분류
- `MediaEncodeInbox`: 영속 멱등성 상태 머신
- `EncodeRequestValidator`: 메시지 schema, key, bucket, storage key 정책
- `MediaProbe`: ffprobe 기반 실제 미디어와 재생 시간 검증
- `FfmpegTranscoder`: 제한 시간 안에서 외부 프로세스 실행과 종료
- `EncodedOutputValidator`: 업로드 전 결과와 HLS 참조 무결성 검증
- `StorageClient`: source metadata/download 및 결과 게시
- `CoreApiClient`: HMAC 인증된 상태 콜백

## Encoding policy

Video preset `VIDEO_HLS_720P_2500K_AAC_96K`:

- H.264, 1280x720, 2500k
- AAC, stereo, 48 kHz, 96k
- 6초 MPEG-TS segment
- VOD manifest `index.m3u8`

Thumbnail preset `THUMBNAIL_1280X720`:

- 1280x720 JPG 한 장

HLS 출력은 manifest가 모든 생성 segment를 안전한 상대 경로로 참조하는지 검사합니다. segment 업로드가 모두 끝난 뒤 manifest를 마지막에 게시합니다.

## Failure policy

- permanent: 메시지/경로/preset 계약 위반, source 없음, 지원하지 않는 미디어, ffmpeg의 결정적 실패, 출력 검증 실패
- retryable: 스토리지 또는 Core API 일시 장애, timeout, 분류할 수 없는 인프라 오류

재시도 가능한 오류는 Inbox에 `RETRY_WAIT`로 남고 retry topic에서 다시 처리됩니다. 영구 오류 또는 재시도 소진은 `FAILURE_PENDING`으로 남은 뒤 실패 콜백에 성공해야 `FAILED`가 됩니다.

## Required tests

- schema JSON 역직렬화와 요청 검증
- Inbox 상태 전이, unique jobId, version, lease 만료 조회
- 중복 수신 시 terminal/busy/callback-only 분기
- 일시 실패에서 실패 콜백을 조기에 보내지 않는지 검증
- 출력 manifest 및 storage path 검증
- HMAC canonical string과 서명 검증
- ffmpeg command/preset 검증

전체 검증은 `./gradlew test`로 실행합니다.
