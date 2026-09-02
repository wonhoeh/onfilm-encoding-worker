# Worker 주요 SQL 추가 Index 적용 전 EXPLAIN 기준선

- 측정일: 2026-09-02
- 대상: OnFilm Encoding Worker DB
- DB: MySQL `8.4.11`, InnoDB
- Schema: Flyway V1~V2
- 상태: 현재 Index 기준선 기록 완료, 신규 Composite Index 적용 보류

## 목적

Repository 메서드 이름만 보고 Index를 추가하지 않고 실제 SQL, 데이터 분포와 실행 계획을 기준으로 다음 질문에 답한다.

- Inbox의 중복 처리, 실패 Callback, lease 복구와 관측성 SQL이 현재 Index를 사용하는가?
- 조회 결과를 얻기 위해 불필요하게 많은 행을 읽거나 정렬하는가?
- 현재 Index의 컬럼 순서는 쿼리 조건과 일치하는가?
- 읽기 성능을 위해 추가 Index의 쓰기·저장 비용을 감수할 근거가 있는가?

이 문서는 신규 Index를 적용하지 않은 V1~V2 상태를 기준선으로 기록한다. Worker에는 초기 Schema부터 운영 조회를 위한 두 Composite Index가 있으므로, 측정 결과 개선 대상이 확인될 때만 다음 Migration을 작성한다.

## 측정 데이터

[기준 데이터 SQL](mysql-index-baseline-setup.sql)은 빈 전용 benchmark DB에만 실행한다. 애플리케이션 Fixture나 운영 데이터로 사용하지 않는다.

| 상태 | 행 수 | lease 분포 |
|---|---:|---|
| `DONE` | 120,000 | 없음 |
| `FAILED` | 20,000 | 없음 |
| `FAILURE_PENDING` | 10,000 | 없음 |
| `PROCESSING` | 20,000 | 만료 200, 유효 19,800 |
| `OUTPUT_UPLOADED` | 10,000 | 만료 100, 유효 4,900, 없음 5,000 |
| `RETRY_WAIT` | 20,000 | 없음 |
| 합계 | 200,000 | - |

만료 작업이 대부분인 비정상 분포에서는 `status`와 `updated_at`만으로도 오래된 행을 빠르게 찾을 수 있어 lease Index의 효과가 가려질 수 있다. 정상 운영에서는 처리 중 작업 가운데 lease 만료가 드물다고 보고 `PROCESSING`의 1%, lease가 있는 `OUTPUT_UPLOADED`의 2%만 만료되도록 구성했다.

`ANALYZE TABLE` 실행 후 동일 쿼리를 한 번 워밍업하고 세 번 측정했다. 실행 시간은 세 결과의 중앙값이며 로컬 장비의 절대 성능이나 운영 SLA로 해석하지 않는다. Index 접근 경로, 실제 읽은 행 수와 상대적 차이가 핵심 근거다.

## 현재 관련 Index

| Index | 컬럼 순서 | 담당 조회 |
|---|---|---|
| `PRIMARY` | `(job_id)` | 메시지 중복 확인과 Inbox 잠금 조회 |
| `idx_inbox_failure_pending` | `(status, updated_at)` | 상태별 오래된 작업 정렬, oldest 메트릭, 상태 건수 |
| `idx_inbox_status_lease` | `(status, lease_until)` | 상태별 만료 lease 복구 |

`idx_inbox_failure_pending`이라는 이름은 최초 사용 목적을 나타내지만 실제로는 모든 상태의 `status + updated_at` 조회를 지원한다. 적용된 V1 Migration은 수정하지 않으며, 기능상 오해나 운영상 문제가 확인되지 않은 현재 단계에서는 이름 변경만을 위한 Index 재생성을 하지 않는다.

## 측정 SQL 선정

| 번호 | Repository 동작 | 선정 이유 |
|---|---|---|
| Q1 | `findByJobIdForUpdate` | `jobId` 멱등 처리와 상태 전이의 시작점 |
| Q2 | `findTop100ByStatusOrderByUpdatedAt` | 실패 Callback 재처리 대상을 오래된 순서로 조회 |
| Q3 | 만료 `PROCESSING` 조회 | Worker 종료 후 lease가 만료된 작업 복구 |
| Q4 | 만료 `OUTPUT_UPLOADED` 조회 | 인코딩 재실행 없이 완료 Callback만 복구 |
| Q5 | `countByStatus` | Inbox 상태별 Gauge 갱신 |
| Q6 | `findOldestUpdatedAtByStatus` | 실패 Callback backlog age 계산 |

Q1의 `FOR UPDATE`는 Index 접근 경로 비교에서 제외했다. 실제 잠금 대기는 별도 MySQL 트랜잭션 통합 테스트가 검증한다. 실행 SQL 전체는 [측정 쿼리](mysql-index-baseline-queries.sql)에 있다.

## EXPLAIN ANALYZE 결과

| SQL | 핵심 접근 경로 | 실제 읽은 행 -> 결과 행 | 실행 시간 3회 | 중앙값 |
|---|---|---:|---:|---:|
| Q1 `job_id` 단건 조회 | Primary Key 단건 결정 | 1 -> 1 | 모두 0.001ms 미만 | 0.001ms 미만 |
| Q2 실패 Callback 100건 | `(status, updated_at)` Index lookup, LIMIT | 100 -> 100 | 1.43 / 1.20 / 1.29ms | 1.29ms |
| Q3 만료 `PROCESSING` | `(status, lease_until)` range scan, 200건 top-N 정렬 | 200 -> 100 | 3.65 / 2.45 / 3.42ms | 3.42ms |
| Q4 만료 `OUTPUT_UPLOADED` | `(status, lease_until)` range scan, 100건 top-N 정렬 | 100 -> 100 | 2.17 / 1.18 / 2.42ms | 2.17ms |
| Q5 `PROCESSING` 건수 | `status` 선두 Composite Index covering lookup | 20,000 -> 1 | 14.0 / 16.8 / 17.6ms | 16.8ms |
| Q6 oldest `FAILURE_PENDING` | `(status, updated_at)` Min/Max optimization | 1 -> 1 | 모두 0.001ms 미만 | 0.001ms 미만 |

### Q1: `jobId` 멱등 조회

`job_id` Primary Key로 한 행을 바로 결정한다. Inbox 최초 claim의 중복 INSERT와 상태 변경의 잠금 기준도 같은 키이므로 추가 Index가 필요하지 않다.

### Q2: 실패 Callback 재처리

`status = FAILURE_PENDING` 동등 조건 뒤에 `updated_at` 정렬 컬럼이 이어지는 `(status, updated_at)` 순서와 쿼리가 일치한다. MySQL은 별도 sort 없이 Index 순서대로 100건을 읽고 LIMIT에서 중단한다.

### Q3~Q4: lease 만료 복구

`status` 동등 조건과 `lease_until < cutoff` 범위를 모두 `(status, lease_until)`에서 처리한다. 전체 `PROCESSING` 20,000건 중 만료 200건, 전체 `OUTPUT_UPLOADED` 10,000건 중 만료 100건만 읽었다.

결과는 `updated_at` 순서이므로 후보 200건 또는 100건에 top-N sort가 남는다. `(status, lease_until, updated_at)`를 추가해도 `lease_until`이 범위 조건이어서 그 뒤의 `updated_at`을 전체 정렬 순서로 활용할 수 없다. 반대로 `(status, updated_at, lease_until)`는 정렬에는 유리하지만 만료 작업이 드문 분포에서 상태에 해당하는 많은 행을 읽으며 lease를 후처리할 수 있다. 현재 후보 수와 측정 시간에서는 세 번째 컬럼을 추가할 근거가 없다.

### Q5: 상태별 건수

Entity 본문을 읽지 않는 covering Index lookup을 사용하지만 정확한 건수를 계산하려면 해당 상태의 Index entry 20,000개를 읽어야 한다. 두 Composite Index가 모두 `status`로 시작하므로 통계와 캐시 상태에 따라 MySQL이 둘 중 하나를 선택할 수 있으며 성능상 같은 역할을 한다. 6개 상태를 주기적으로 집계하는 현재 규모에서는 허용하되, 데이터가 크게 늘어 이 Gauge 갱신 자체가 부하가 되면 별도 집계 테이블이나 이벤트 기반 카운터를 검토한다. 정확한 `COUNT` 하나만을 위해 중복 Index를 추가하지 않는다.

### Q6: 가장 오래된 실패 Callback

`(status, updated_at)`의 상태별 첫 entry에서 최솟값을 결정하는 Min/Max optimization이 적용됐다. `FAILURE_PENDING` 10,000건을 모두 집계하지 않고 실행 전에 한 건으로 결과를 결정하므로 추가 Index가 필요하지 않다.

## 신규 Composite Index 판단

| 후보 | 판단 | 이유 |
|---|---|---|
| `(status, updated_at)` | 기존 Index 유지 | 정렬 조회, MIN과 COUNT를 지원 |
| `(status, lease_until)` | 기존 Index 유지 | 드문 만료 작업만 range scan으로 축소 |
| `(status, lease_until, updated_at)` | 제외 | lease 범위 뒤 컬럼으로 전체 ORDER BY를 제거할 수 없음 |
| `(status, updated_at, lease_until)` | 제외 | 정렬을 위해 만료되지 않은 상태 행까지 읽을 가능성이 큼 |
| 상태별 단일 컬럼 Index | 제외 | 두 기존 Composite Index의 왼쪽 prefix와 중복 |

현재 주요 SQL은 Primary Key 또는 두 기존 Composite Index로 지원된다. 따라서 측정 없이 Index 수를 늘리는 V3 Migration은 작성하지 않는다. Index는 조회 비용을 줄이는 대신 Inbox INSERT와 `status`, `updated_at`, `lease_until` 변경 시 B-Tree 갱신, 디스크와 Buffer Pool 사용량을 늘리기 때문이다.

운영 데이터 분포, 호출 빈도 또는 slow query log에서 병목이 확인되면 같은 SQL과 분포로 다시 측정한다. 특히 만료 후보가 수만 건으로 늘어난다면 이는 Index보다 장애 적체와 복구 batch 정책을 먼저 점검할 신호다.

## 재현 절차

전용 임시 컨테이너에서만 실행한다. 기준 데이터 SQL은 `media_encode_inbox`를 비우므로 개발·운영 DB에서 실행하면 안 된다.

```bash
docker run --name onfilm-worker-index-baseline \
  -e MYSQL_ROOT_PASSWORD=onfilm_benchmark_password \
  -e MYSQL_DATABASE=onfilm_worker \
  -d mysql:8.4.11
```

V1부터 V2까지 적용한다.

```bash
for migration in \
  src/main/resources/db/migration/V1__create_initial_schema.sql \
  src/main/resources/db/migration/V2__strengthen_inbox_constraints.sql
do
  docker exec -i onfilm-worker-index-baseline \
    mysql -uroot -ponfilm_benchmark_password onfilm_worker \
    < "$migration"
done
```

기준 데이터를 적재하고 실행 계획을 측정한다.

```bash
docker exec -i onfilm-worker-index-baseline \
  mysql -uroot -ponfilm_benchmark_password \
  < docs/performance/mysql-index-baseline-setup.sql

docker exec -i onfilm-worker-index-baseline \
  mysql -uroot -ponfilm_benchmark_password --table \
  < docs/performance/mysql-index-baseline-queries.sql
```

측정이 끝나면 임시 컨테이너를 제거한다.

```bash
docker rm -f onfilm-worker-index-baseline
```

## 관련 문서

- [Worker MySQL Testcontainers 환경](../WORKER_MYSQL_TESTCONTAINERS.md)
- [Worker MySQL Constraint 감사](../WORKER_MYSQL_CONSTRAINT_AUDIT.md)
- [Worker 신뢰성 결정](../WORKER_RELIABILITY_DECISIONS.md)
- [Worker DB Schema 변경 규칙](../../AGENTS.md)
