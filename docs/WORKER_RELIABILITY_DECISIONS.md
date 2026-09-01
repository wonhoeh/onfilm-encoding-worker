# Worker reliability decisions

## Persistent Inbox

Kafka 전달은 at-least-once이므로 같은 `jobId`가 다시 전달될 수 있다. 워커는 `media_encode_inbox`를 로컬 메모리가 아닌 DB에 저장하고 `jobId`를 기본 키로 사용한다.

- `PROCESSING`: 작업 lease를 획득해 변환 중
- `RETRY_WAIT`: 재시도 가능한 실패 후 Kafka 재전달 대기
- `OUTPUT_UPLOADED`: 업로드는 끝났고 완료 콜백만 남음
- `FAILURE_PENDING`: 재시도 종료 후 실패 콜백 대기
- `DONE`, `FAILED`: 종결 상태이며 중복 메시지는 무시

프로세스가 종료되더라도 만료된 lease와 원본 Kafka payload를 이용해 작업을 복구한다. 출력 업로드 뒤 콜백에 실패한 경우에는 인코딩과 업로드를 반복하지 않고 완료 콜백만 재시도한다.

복구 작업은 긴 시간 scheduler thread를 점유할 수 있으므로 scheduler pool을 2개로 두어 실패 콜백 재시도와 서로 막지 않게 한다. processing lease는 transcode timeout뿐 아니라 다운로드와 업로드의 최악 실행 시간까지 포함하도록 운영값을 잡아야 한다.

이 방식은 DB 운영과 상태 정리 비용이 생기지만, 긴 인코딩을 중복 실행하거나 이미 업로드한 결과를 다시 만드는 비용을 줄인다.

### Inbox Schema management

Worker는 `onfilm_worker` 논리 DB와 `media_encode_inbox` 테이블만 소유한다. Schema의 단일 기준은 Flyway Versioned Migration이며, MySQL 환경에서 Hibernate는 `ddl-auto: validate`만 수행한다.

- `job_id`는 Kafka·Callback의 UUID 문자열과 같은 `VARCHAR(36)` PK로 저장한다.
- API Job과 값은 연결하지만 API DB를 참조하는 FK는 만들지 않는다.
- 원본 Kafka JSON은 `TEXT`로 저장해 lease 만료와 Callback-only 복구에 사용한다.
- `(status, lease_until)`은 만료 lease 조회, `(status, updated_at)`은 실패 보고와 운영 조회를 지원한다.
- V1은 보존할 운영 데이터가 없는 빈 `onfilm_worker`를 대상으로 하며 `baselineOnMigrate`를 사용하지 않는다.
- 개발용 H2는 빠른 확인에만 사용하고 Migration·MySQL 호환성의 근거로 삼지 않는다.

## Retry and DLT

네트워크, S3, Core API 5xx/408/429, ffmpeg timeout 같은 일시 오류는 지수 backoff로 재시도한다. 계약 위반, 허용되지 않은 경로·bucket, 지원하지 않는 preset, 유효하지 않은 출력은 영구 실패로 분류한다.

최종 실패는 DLT 처리 또는 주기적인 `FAILURE_PENDING` 복구 작업에서 Core API에 보고한다. 실패 보고 자체가 실패하면 Inbox에 남겨 다음 주기에 다시 시도한다. 원본 메시지에 `jobId`가 없으면 작업을 식별할 수 없으므로 로그만 남긴다.

DLT는 자동으로 원래 topic에 되돌리지 않는다. DLT handler는 수동 조사와 단건 재처리를 위해 DLT와 원본의 `topic`, `partition`, `offset`, 실패 유형과 정제된 실패 메시지를 구조화 로그로 남긴다. 실패 메시지의 인증·토큰·비밀값 패턴은 제거하고 stacktrace header 전체는 기록하지 않는다.

수동 재처리는 Core API Job이 `REQUESTED` 또는 `PROCESSING`이고 Inbox가 재개 가능한 상태일 때만 기존 `jobId`로 허용한다. API Job이나 Inbox가 `DONE` 또는 `FAILED`이면 최종 상태를 되돌리지 않는다. `OUTPUT_UPLOADED`는 인코딩을 다시 실행하지 않고 Callback-only 복구를 사용한다.

## Callback authentication

고정 Bearer token 대신 요청마다 HMAC-SHA256 서명을 생성한다. 서명 대상은 timestamp, nonce, HTTP method, raw path, body SHA-256이다.

필수 헤더:

- `X-Onfilm-Timestamp`: epoch seconds
- `X-Onfilm-Nonce`: 매 요청마다 새 UUID
- `X-Onfilm-Signature`: lowercase hexadecimal HMAC-SHA256

운영 비밀값은 `MEDIA_ENCODE_CALLBACK_SECRET`으로 주입하며 로그에 남기지 않는다. 서버는 timestamp 허용 오차와 nonce 재사용 여부도 검증해야 한다.

## Storage publication

HLS 세그먼트를 먼저 업로드하고 manifest(`index.m3u8`)를 마지막에 올린다. 소비자가 manifest를 발견한 시점에 참조 파일이 준비되어 있을 가능성을 높이기 위한 정책이다. 업로드 도중 실패하면 이번 시도에서 올린 정확한 key만 정리한다.
