# onfilm-encoding-worker

Kafka의 `media.encode.requested` 메시지를 받아 원본 미디어를 검증하고, ffmpeg로 HLS 또는 썸네일을 생성한 뒤 스토리지에 게시하는 비동기 워커입니다. 작업 상태와 최종 미디어 반영은 Core API가 소유하며 워커는 서명된 내부 콜백으로 결과를 전달합니다.

## 처리 흐름

```text
Kafka message
  -> persistent Inbox claim
  -> message/storage-key validation
  -> PROCESSING callback
  -> source metadata + download
  -> ffprobe validation
  -> ffmpeg transcode with timeout
  -> output validation
  -> segments/files upload, HLS manifest last
  -> OUTPUT_UPLOADED checkpoint
  -> COMPLETE callback
  -> DONE
```

Kafka는 at-least-once 전달을 전제로 합니다. `jobId` 기반 DB Inbox가 중복 작업을 막고, lease 만료 작업을 복구합니다. 업로드 후 콜백만 실패한 경우에는 변환을 다시 하지 않고 완료 콜백만 재시도합니다. 재시도 가능한 실패는 retry topic으로, 최종 실패는 DLT 및 실패 콜백 복구 스케줄러로 처리합니다.

자세한 선택 근거는 [Worker reliability decisions](docs/WORKER_RELIABILITY_DECISIONS.md), 서버 계약은 [API Server Media Job Status Spec](docs/API_SERVER_MEDIA_JOB_STATUS_SPEC.md)을 참고합니다.

## Kafka 계약

- topic: `media.encode.requested`
- message key: `jobId` 문자열
- schema version: `1`

```json
{
  "schemaVersion": 1,
  "jobId": "11111111-2222-3333-4444-555555555555",
  "requestId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
  "movieId": 123,
  "requestedByUserId": 45,
  "jobType": "MOVIE",
  "preset": "VIDEO_HLS_720P_2500K_AAC_96K",
  "sourceBucket": "onfilm-media",
  "sourceKey": "movie/123/raw/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/input.mp4",
  "targetBucket": "onfilm-media",
  "targetKey": "movie/123/file/99999999-8888-7777-6666-555555555555/index.m3u8",
  "sourceContentType": "video/mp4",
  "targetContentType": "application/vnd.apple.mpegurl",
  "requestedAt": "2026-08-21T00:00:00Z"
}
```

허용 조합:

| jobType | preset | target |
| --- | --- | --- |
| `MOVIE` | `VIDEO_HLS_720P_2500K_AAC_96K` | `movie/{movieId}/file/{outputId}/index.m3u8` |
| `TRAILER` | `VIDEO_HLS_720P_2500K_AAC_96K` | `movie/{movieId}/trailer/{outputId}/index.m3u8` |
| `THUMBNAIL` | `THUMBNAIL_1280X720` | `movie/{movieId}/thumbnail/{outputId}.jpg` |

`outputId`는 서버가 target key를 만들 때 별도로 발급하는 UUID이며 upload `requestId`와 같을 필요는 없습니다.

source와 target bucket은 `app.storage.allowed-buckets`에 있어야 하며 key의 절대 경로, `.`/`..`, 역슬래시는 거부합니다.

## 실행 요구 사항

- Java 17
- Kafka
- ffmpeg 및 ffprobe
- S3 또는 개발용 local storage
- Inbox용 H2/MySQL DB
- Core API와 공유하는 32자 이상의 HMAC secret

개발 프로필은 메모리 H2와 local storage를 사용합니다.

```bash
MEDIA_ENCODE_CALLBACK_SECRET='<32자 이상의 개발용 값>' \
SPRING_PROFILES_ACTIVE=dev ./gradlew bootRun
```

운영 시 최소 환경 변수:

```text
SPRING_KAFKA_BOOTSTRAP_SERVERS
MEDIA_ENCODE_CALLBACK_SECRET
MEDIA_STORAGE_BUCKET
WORKER_DB_URL
WORKER_DB_USER
WORKER_DB_PASSWORD
```

Inbox는 장애 복구에 필요하므로 운영에서 휘발성 DB를 사용하면 안 됩니다. 여러 worker 인스턴스는 동일한 Inbox DB를 공유해야 합니다.

주요 설정:

```yaml
app:
  worker:
    transcode-timeout: PT2H
    processing-lease: PT3H
    max-source-bytes: 10737418240
    max-media-duration: PT6H
    concurrency: 1
    retry-attempts: 5
    retry-delay-millis: 1000
  storage:
    type: s3
    allowed-buckets: [onfilm-media]
  core-api:
    base-url: http://localhost:8080
    callback-secret: ${MEDIA_ENCODE_CALLBACK_SECRET}
```

`processing-lease`는 `transcode-timeout`보다 길어야 합니다. Kafka `max.poll.interval.ms`도 최장 처리 시간보다 길게 설정해야 합니다.

## 관측성과 운영

- health: `/actuator/health`, `/actuator/health/liveness`, `/actuator/health/readiness`
- metrics: `/actuator/metrics`
- Prometheus: `/actuator/prometheus`
- 주요 지표: `media.encode.completed`, `media.encode.failed`, `media.encode.duplicate`, `media.encode.duration`
- 로그 MDC: `jobId`, `requestId`

health check는 작업 디렉터리 쓰기 가능 여부, 가용 디스크 1 GiB 이상, 절대 경로로 설정한 ffmpeg의 실행 가능 여부를 검사합니다. 시작 시 오래된 UUID 작업 디렉터리를 정리합니다.

## 검증

```bash
./gradlew test
./gradlew integrationTest
./gradlew check
```

DB 동작이 중요한 통합 테스트는 [MySQL Testcontainers 환경](docs/WORKER_MYSQL_TESTCONTAINERS.md)에서 실행합니다. 빈 `onfilm_worker`에 Flyway Migration을 적용하고 Hibernate `validate`로 Inbox Mapping을 검증합니다. Unique·Nullable·FK와 상태 조합의 판단 근거는 [Worker MySQL Constraint 감사](docs/WORKER_MYSQL_CONSTRAINT_AUDIT.md)에 기록합니다. 주요 Inbox SQL의 실행 계획과 Composite Index 컬럼 순서의 근거는 [MySQL Index 기준선](docs/performance/mysql-index-baseline.md)에서 확인할 수 있습니다.

Pull Request와 `main`, `test` 브랜치 Push에서는 [Worker CI](.github/workflows/worker-ci.yml)가 단위 테스트와 MySQL 통합 테스트를 별도 Job으로 병렬 실행합니다. 테스트가 실패하면 해당 Gradle 보고서를 7일간 Artifact로 보관합니다.
