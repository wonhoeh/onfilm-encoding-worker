# Worker MySQL Unique·Nullable·FK Constraint 감사

- 감사일: 2026-09-02
- 대상 DB: `onfilm_worker`
- 대상 테이블: `media_encode_inbox`
- 기준 Migration: `V1__create_initial_schema.sql`부터 `V2__strengthen_inbox_constraints.sql`까지
- 기준 실행 환경: MySQL 8.4.11, `utf8mb4_0900_ai_ci`
- 상태: 감사 완료, V2 Constraint와 MySQL 거부 테스트 적용 완료

## 목적

Worker의 JPA Mapping, Flyway Schema와 실제 상태 전이를 대조해 다음 질문에 답한다.

- Kafka의 at-least-once 전달에서도 작업 멱등성 키가 DB에서 보호되는가?
- nullable 컬럼이 누락된 제약인지, 복구 상태를 표현하기 위한 의도인지 구분되는가?
- API Job과 Worker Inbox 사이에 FK를 추가해야 하는가?
- 직접 SQL이나 동시 처리로도 깨지면 안 되는 상태 불변식을 CHECK로 보강할 수 있는가?
- 제약 강화가 정상적인 retry, lease 복구와 Callback-only 흐름을 막지 않는가?

V1 상태를 감사한 뒤 적용된 V1은 수정하지 않고, 확정한 제약과 MySQL 거부 테스트를 별도 V2 Migration 작업으로 추가했다.

## 현재 Schema 현황

| 항목 | 개수 | 현재 상태 |
|---|---:|---|
| Worker 소유 테이블 | 1 | `media_encode_inbox` |
| Primary Key | 1 | `job_id` |
| PK 외 Unique Constraint | 0 | 추가 중복 키 없음 |
| 명시적 Check Constraint | 10 | 수치·시간·payload·상태 조합 보호 |
| MySQL ENUM 컬럼 | 2 | `status`, `failure_code` |
| Foreign Key | 0 | API DB와 물리 관계 없음 |
| nullable 컬럼 | 3 | `lease_until`, `failure_code`, `failure_reason` |
| 보조 Index | 2 | lease 복구, 실패 Callback 조회 |

`job_id` Primary Key가 한 Job당 Inbox 한 건만 허용하므로 Worker의 핵심 멱등성은 DB에서도 보호된다. 별도 Unique Constraint는 현재 필요하지 않다. V1에서 애플리케이션 로직에만 의존하던 횟수, 시간 순서와 상태별 nullable 조합은 V2 CHECK로 보강했다.

## 컬럼별 감사 결과

| 컬럼 | V1 정의 | 도메인 의미 | 판정 |
|---|---|---|---|
| `job_id` | `VARCHAR(36) NOT NULL PK` | Kafka·Callback의 Worker 멱등성 키 | 유지 |
| `version` | `BIGINT NOT NULL` | 중복 처리와 복구 경쟁의 낙관적 락 버전 | 0 이상 CHECK 보강 |
| `status` | `ENUM NOT NULL` | Inbox 상태 머신 | enum 유지, 상태 조합 CHECK 보강 |
| `attempts` | `INT NOT NULL` | 최초 처리를 포함한 claim 횟수 | 1 이상 CHECK 보강 |
| `lease_until` | nullable | 처리권 만료 또는 Callback 재개 시각 | nullable 유지, 상태·시간 CHECK 보강 |
| `created_at` | `DATETIME(6) NOT NULL` | Inbox 최초 생성 시각 | 유지 |
| `updated_at` | `DATETIME(6) NOT NULL` | 마지막 상태 변경 시각 | 생성 시각 이상 CHECK 보강 |
| `kafka_key` | `VARCHAR(36) NOT NULL` | 최초 수신 Kafka key와 중복 요청 동일성 비교 | 유지, UNIQUE·job_id 일치 CHECK 제외 |
| `payload` | `TEXT NOT NULL` | 복구와 충돌 판정을 위한 직렬화 메시지 | TEXT 유지, JSON 유효성 CHECK 적용 |
| `failure_code` | nullable ENUM | 실패가 기록된 상태의 분류 코드 | nullable 유지, failure pair CHECK 보강 |
| `failure_reason` | nullable `VARCHAR(1000)` | 정제·길이 제한된 운영 실패 사유 | nullable 유지, failure pair·blank CHECK 보강 |

### UUID와 Kafka key

`job_id`는 Java `UUID`를 Hibernate `VARCHAR` 타입으로 저장하므로 canonical UUID 문자열을 사용한다. 기본 Collation은 대소문자를 구분하지 않지만 UUID 생성 값이 소문자 canonical 형식으로 제한되어 현재 PK 의미와 충돌하지 않는다.

정상 메시지는 Kafka key와 payload의 `jobId`가 같아야 한다. 그러나 현재 처리 흐름은 Inbox claim 이후 요청 검증을 수행하므로, 잘못된 Kafka key도 실패 추적과 중복 판정을 위해 Inbox에 기록될 수 있다. 따라서 다음 제약은 추가하지 않는다.

- `UNIQUE(kafka_key)`: `job_id` PK와 정상 요청에서는 중복이고, 잘못된 요청 기록을 불필요하게 충돌시킨다.
- `CHECK(kafka_key = job_id)`: 요청 검증 책임과 실패 기록 흐름을 DB 쓰기 오류로 바꾼다.

Kafka key 일치 여부는 `EncodeRequestValidator`가 명확한 실패 코드로 처리하고, DB는 최초 수신 key가 변조되지 않도록 NOT NULL·updatable false Mapping을 유지한다.

### Payload 저장 형식

`payload`는 조회 조건으로 사용하지 않고 Worker 복구 시 전체 메시지를 역직렬화하는 원문 스냅샷이다. MySQL `JSON` 컬럼으로 변경하면 JPA 타입 Mapping과 저장 표현의 결합이 커지므로 `TEXT`를 유지한다. 직접 SQL로 손상된 문자열이 저장되는 것은 V2의 `CHECK(JSON_VALID(payload))`로 차단한다.

## Unique Constraint 감사

| 후보 | 결정 | 이유 |
|---|---|---|
| `PRIMARY KEY(job_id)` | 유지 | 동일 Job의 중복 INSERT를 DB가 최종 차단 |
| `UNIQUE(kafka_key)` | 추가하지 않음 | 정상 요청에서는 PK와 중복이며 invalid key 기록을 방해 |
| `UNIQUE(payload)` | 추가하지 않음 | 다른 Job이 동일한 메시지 형태를 갖는 것은 DB 중복이 아님 |
| payload hash Unique | 추가하지 않음 | 멱등성 계약은 메시지 내용이 아니라 API가 발급한 `jobId` 기준 |

서비스의 사전 조회와 비관적 잠금은 의미 있는 `BUSY`, `TERMINAL`, `CALLBACK_ONLY` 결과를 만들기 위한 처리다. 동시에 최초 메시지가 도착하는 경쟁 조건의 최종 방어선은 `job_id` PK이며, 애플리케이션 조회만으로 대체하지 않는다.

## Nullable과 상태 조합 감사

nullable 세 컬럼은 모두 상태 머신에 필요한 의도적인 선택이다. 무조건 NOT NULL로 변경하지 않는다.

| 상태 | `lease_until` | 실패 코드·사유 | 이유 |
|---|---|---|---|
| `PROCESSING` | 필수 | 선택 | retry claim 후 이전 실패 정보를 추적용으로 유지할 수 있음 |
| `RETRY_WAIT` | null | 필수 | 처리 lease를 반납하고 재시도 가능한 실패를 기록 |
| `OUTPUT_UPLOADED` | 선택 | 선택 | Callback 진행 중에는 lease가 있고 Callback 실패 후에는 null 가능 |
| `FAILURE_PENDING` | null | 필수 | 실패 Callback 전송에 필요한 정보 보존 |
| `DONE` | null | null | 성공 종결 시 lease와 과거 실패 정보 제거 |
| `FAILED` | null | 필수 | 실패 종결 원인 보존 |

다음 규칙은 현재 모든 Entity 전이와 일치하며 V2 CHECK로 적용했다.

1. `attempts >= 1`
2. `version >= 0`
3. `updated_at >= created_at`
4. lease가 있으면 `lease_until > updated_at`
5. `failure_code`와 `failure_reason`은 둘 다 null이거나 둘 다 값이 있어야 함
6. 실패 사유가 있으면 trim 기준 blank가 아니어야 함
7. `PROCESSING`에는 lease가 반드시 있어야 함
8. `RETRY_WAIT`, `FAILURE_PENDING`, `DONE`, `FAILED`에는 lease가 없어야 함
9. `RETRY_WAIT`, `FAILURE_PENDING`, `FAILED`에는 실패 코드와 사유가 있어야 함
10. `DONE`에는 실패 코드와 사유가 없어야 함

`OUTPUT_UPLOADED`에는 lease와 실패 정보를 단일 형태로 강제하지 않는다. 출력 업로드 직후에는 Callback lease가 있지만, retryable Callback 실패를 기록하면 재변환 없이 Callback만 다시 실행하기 위해 같은 상태에서 lease가 null이 된다.

## Foreign Key와 삭제 정책 감사

`media_encode_inbox.job_id`는 API DB의 `media_encode_jobs.job_id`와 같은 값을 사용하지만 Foreign Key가 아니다.

| 관계 후보 | 결정 | 이유 |
|---|---|---|
| Inbox → API Job | FK 추가하지 않음 | 서로 다른 논리 DB와 소유 저장소이며 Cross-DB FK 금지 |
| Inbox → Movie | 컬럼·FK 추가하지 않음 | movieId는 payload 스냅샷에 있고 Worker 조회 관계가 아님 |
| Inbox → User | 컬럼·FK 추가하지 않음 | requestedByUserId는 감사 정보이며 Worker 소유 관계가 아님 |

API Job, Movie 또는 User가 삭제돼도 Worker Inbox를 DB cascade로 즉시 제거하지 않는다. Kafka 중복 전달, 장애 복구와 Callback 추적에는 독립 보존 기간이 필요하다. Worker Inbox 삭제는 추후 retention 정책과 Worker 전용 유지보수 작업이 책임진다.

이 선택의 대가는 API에 없는 Job의 Inbox가 일정 기간 남을 수 있다는 점이다. 이를 고아 데이터로 간주해 FK로 제거하는 대신 terminal 상태·보존 기한·정리 메트릭으로 운영하며, API DB를 직접 조회하거나 두 DB를 하나의 트랜잭션으로 묶지 않는다.

## MySQL ENUM 정책

`status`와 `failure_code`는 V1에서 MySQL ENUM으로 정의되어 허용하지 않은 문자열 저장을 차단한다. 현재 테이블이 하나이고 값 집합이 명확하므로 유지한다.

트레이드오프는 enum 값 추가·이름 변경이 Java 코드만의 변경으로 끝나지 않는다는 것이다. 앞으로 `InboxStatus` 또는 `FailureCode`를 변경할 때는 다음을 한 작업 단위로 수행한다.

1. 새 Flyway Migration에서 MySQL ENUM 값 변경
2. Java enum과 상태 전이 변경
3. 기존 저장 값의 변환 또는 호환성 검토
4. 빈 MySQL Migration과 Hibernate `validate` 실행
5. 모든 상태 저장과 거부 테스트 재실행

## 적용된 Constraint Migration

적용된 V1을 수정하지 않고 `V2__strengthen_inbox_constraints.sql`을 추가했다.

### 적용 대상

- `ck_inbox_attempts_positive`
- `ck_inbox_version_non_negative`
- `ck_inbox_timestamp_order`
- `ck_inbox_lease_after_update`
- `ck_inbox_failure_pair`
- `ck_inbox_failure_reason_not_blank`
- `ck_inbox_lease_status`
- `ck_inbox_failure_status`
- `ck_inbox_done_clears_failure`
- `ck_inbox_payload_json`

Constraint 이름은 실패 응답과 운영 로그에서 원인을 식별할 수 있도록 고정한다. 상태 조합은 하나의 거대한 CHECK보다 책임별 제약으로 나눠 어떤 불변식이 깨졌는지 알 수 있게 한다.

### 적용된 거부 테스트

V2 적용 후 MySQL에서 다음 직접 INSERT·UPDATE가 각각 거부되는지 검증한다.

- attempts 0과 version 음수
- 생성보다 이른 updated 시각과 updated 시각 이하의 lease
- 코드만 있거나 사유만 있는 failure pair
- blank failure reason
- lease가 없는 PROCESSING
- lease가 남아 있는 RETRY_WAIT·FAILURE_PENDING·DONE·FAILED
- 실패 정보가 없는 RETRY_WAIT·FAILURE_PENDING·FAILED
- 실패 정보가 남은 DONE
- JSON이 아닌 payload

정상 경로는 여섯 Inbox 상태와 Callback 실패 후 `OUTPUT_UPLOADED` 변형을 모두 실제로 저장한다. 기존 duplicate delivery, lease 복구, Callback-only, terminal 상태와 비관적 잠금 테스트도 전체 `check`에서 재실행한다.

## 의도적으로 적용하지 않는 제약

- API DB를 참조하는 Cross-DB FK와 cascade
- `kafka_key` 또는 `payload`의 추가 UNIQUE
- Kafka key와 `job_id`의 DB 일치 CHECK
- `lease_until`, `failure_code`, `failure_reason`의 일괄 NOT NULL
- API Job·Movie·User 삭제와 Worker Inbox의 동기 삭제
- 부모가 없는 단일 Aggregate에 의미 없는 연관관계 제약

## 완료 기준

1. 빈 MySQL에 V1부터 V2까지 순서대로 적용된다.
2. Hibernate `ddl-auto: validate`가 통과한다.
3. 각 신규 CHECK에 정상 경계값과 거부 사례가 존재한다.
4. 여섯 Inbox 상태의 합법적인 저장과 전이가 모두 통과한다.
5. 기존 Repository, transaction, duplicate, lease와 Callback 복구 테스트가 통과한다.
6. API·Worker 사이에 FK·JOIN·직접 조회가 추가되지 않는다.
7. `./gradlew test`, `./gradlew integrationTest`, 최종 `./gradlew check`가 통과한다.

## 참고 문서

- [Worker 신뢰성 결정](WORKER_RELIABILITY_DECISIONS.md)
- [Worker MySQL Testcontainers 환경](WORKER_MYSQL_TESTCONTAINERS.md)
- [Worker DB Schema 변경 규칙](../AGENTS.md)
- [API·Worker DB 소유권과 Flyway 정책](https://github.com/wonhoeh/onfilm/blob/main/docs/decisions/api-worker-database-ownership-and-flyway-baseline-policy.md)
- [API MySQL Constraint 감사](https://github.com/wonhoeh/onfilm/blob/main/docs/review/database/mysql-constraint-audit.md)
