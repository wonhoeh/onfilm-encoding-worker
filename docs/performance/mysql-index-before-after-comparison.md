# Worker 주요 SQL Index 유무 성능 비교

- 측정일: 2026-09-02
- 대상: OnFilm Encoding Worker DB
- DB: MySQL `8.4.11`, InnoDB
- Schema: Flyway V1~V2
- 데이터: `media_encode_inbox` 200,000건
- 결론: 기존 V1 Composite Index 유지, 신규 Index Migration 생략

## 목적

[추가 Index 적용 전 기준선](mysql-index-baseline.md)에서 Worker의 주요 SQL은 기존 두 Composite Index로 지원되고 추가 후보는 효과가 불명확하다고 판단했다. 이번 측정은 다음을 확인한다.

- 기존 Index가 없으면 실제로 전체 스캔이 발생하는가?
- 기존 Index가 읽는 행 수와 실행 시간을 얼마나 줄이는가?
- 새 Index를 추가하지 않고 V1 Index를 유지한다는 결정에 측정 근거가 있는가?

Worker의 두 보조 Index는 V1부터 존재하므로 Migration 적용 전후 DB를 비교한 것이 아니다. 전용 benchmark DB에서 Index를 `INVISIBLE`로 바꿔 Optimizer가 사용하지 못하게 한 상태와 다시 `VISIBLE`로 복구한 상태를 같은 데이터와 SQL로 비교했다.

## 비교 대상

| 구분 | Index 상태 | 의미 |
|---|---|---|
| 미적용 조건 | 두 보조 Index `INVISIBLE` | Primary Key 외에는 Optimizer가 사용하지 않음 |
| 적용 조건 | 두 보조 Index `VISIBLE` | 실제 V1 Schema와 같은 상태 |

비교한 Index는 다음과 같다.

| Index | 컬럼 순서 | 주된 책임 |
|---|---|---|
| `idx_inbox_failure_pending` | `(status, updated_at)` | 실패 Callback 정렬, 가장 오래된 대기 시각, 상태 집계 |
| `idx_inbox_status_lease` | `(status, lease_until)` | 상태별 만료 lease 탐색 |

Invisible Index는 조회 Optimizer의 선택에서만 제외되며 디스크에서 제거되거나 INSERT·UPDATE 유지 비용이 사라지지는 않는다. 따라서 이번 실험은 조회 접근 경로와 읽기 성능을 비교하며, Index의 저장 공간과 쓰기 비용을 수치화한 실험은 아니다.

## 측정 조건

- [기준 데이터 SQL](mysql-index-baseline-setup.sql)로 20만 건 생성
- `PROCESSING` 20,000건 중 lease 만료 200건
- `OUTPUT_UPLOADED` 10,000건 중 lease 만료 100건
- 각 조건에서 `ANALYZE TABLE` 실행
- 동일 쿼리를 한 번 워밍업한 후 세 번 `EXPLAIN ANALYZE`
- 세 실행 시간의 중앙값 비교

두 조건을 동일 컨테이너에서 순서대로 측정했으므로 Buffer Pool과 OS cache의 영향을 완전히 제거한 절대 성능 실험은 아니다. 실행 시간 배수는 참고값이며, 접근 경로와 실제 읽은 행 수 감소를 주된 근거로 삼는다.

## 비교 결과

Q1 `job_id` 단건 조회는 두 조건 모두 Primary Key로 한 행을 읽으므로 비교에서 제외했다.

| SQL | Index 미적용 중앙값 | Index 적용 중앙값 | 시간 개선 | 읽은 행 변화 |
|---|---:|---:|---:|---:|
| Q2 실패 Callback 100건 | 129ms | 0.753ms | 약 171배 | 200,000 -> 100 |
| Q3 만료 `PROCESSING` 100건 | 124ms | 1.88ms | 약 66배 | 200,000 -> 200 |
| Q4 만료 `OUTPUT_UPLOADED` 100건 | 125ms | 0.932ms | 약 134배 | 200,000 -> 100 |
| Q5 `PROCESSING` 건수 | 51.0ms | 7.95ms | 약 6.4배 | 200,000 -> 20,000 |
| Q6 oldest `FAILURE_PENDING` | 53.8ms | 0.001ms 미만 | 최소 53,800배 | 200,000 -> 1 |

### Q2: 실패 Callback 재처리

Index가 없으면 20만 행을 모두 읽어 `FAILURE_PENDING` 10,000건을 찾고 `updated_at`으로 top-N 정렬한다. `(status, updated_at)`가 활성화되면 상태와 정렬 순서가 Index에 함께 있으므로 앞의 100건만 읽고 중단한다.

- 실행 시간: `129ms -> 0.753ms`
- 읽은 행: `200,000 -> 100`, 99.95% 감소
- 별도 정렬: 제거

### Q3~Q4: lease 만료 복구

Index가 없으면 만료 작업이 100~200건뿐이어도 매번 20만 행을 스캔한다. `(status, lease_until)`가 활성화되면 상태와 만료 시각 범위를 Index에서 처리해 실제 만료 후보만 읽는다.

`updated_at` top-N 정렬은 남지만 입력이 Q3 200건, Q4 100건으로 제한된다. 세 번째 정렬 컬럼을 Index에 추가하는 것보다 현재의 작은 후보 집합을 정렬하는 편이 쓰기 비용과 조회 효과의 균형에 맞는다.

- Q3 읽은 행: `200,000 -> 200`, 99.9% 감소
- Q4 읽은 행: `200,000 -> 100`, 99.95% 감소

### Q5: 상태별 건수

Index가 없으면 20만 행 전체를 읽고 `PROCESSING` 20,000건을 센다. Index가 활성화되면 `status`가 선두인 작은 보조 Index만 읽는 covering lookup을 사용한다.

- 실행 시간: `51.0ms -> 7.95ms`
- 읽은 행: `200,000 -> 20,000`, 90% 감소

정확한 건수는 일치하는 Index entry를 모두 읽어야 하므로 데이터가 계속 증가하면 선형 비용이 남는다. 현재 단계에서는 추가 Index가 해결책이 아니며, 메트릭 수집 부하가 확인될 때 집계 테이블이나 이벤트 기반 카운터를 검토한다.

### Q6: 가장 오래된 실패 Callback

Index가 없으면 20만 행을 스캔해 `FAILURE_PENDING` 10,000건의 최솟값을 계산한다. `(status, updated_at)`가 활성화되면 MySQL의 Min/Max optimization으로 상태별 첫 Index entry 한 건에서 결과를 결정한다.

- 실행 시간: `53.8ms -> 0.001ms 미만`
- 읽은 행: `200,000 -> 1`

## 결정

두 V1 Composite Index는 다음 운영 기능에 필요하므로 유지한다.

- 실패 Callback의 오래된 순서 재처리와 backlog age 관측
- Worker 장애 후 `PROCESSING` lease 복구
- 업로드 완료 후 Callback-only 복구
- Inbox 상태별 메트릭 집계

반면 현재 주요 SQL은 이미 전체 스캔을 피하고 있고, 기준선에서 검토한 세 번째 컬럼 후보도 정렬 제거를 보장하지 않는다. 조회 성능 개선이 검증되지 않은 Index는 Inbox INSERT와 상태 변경의 B-Tree 갱신 비용만 늘릴 수 있으므로 신규 V3 Index Migration을 만들지 않는다.

이는 성능 작업을 생략한 것이 아니라, 같은 데이터와 SQL로 측정한 결과 변경하지 않는 편이 더 낫다고 판단한 것이다. 이후 운영 slow query, 데이터 분포 또는 조회 계약이 달라지면 이 결과를 기준선으로 다시 비교한다.

## 재현 절차

전용 임시 컨테이너와 [기준선 재현 절차](mysql-index-baseline.md#재현-절차)로 V1~V2 및 20만 건 데이터를 준비한다. 운영·개발 DB에서는 실행하지 않는다.

두 보조 Index를 Optimizer에서 숨긴 뒤 워밍업하고 세 번 측정한다.

```bash
docker exec onfilm-worker-index-comparison \
  mysql -uroot -ponfilm_benchmark_password onfilm_worker \
  -e "ALTER TABLE media_encode_inbox
      ALTER INDEX idx_inbox_failure_pending INVISIBLE,
      ALTER INDEX idx_inbox_status_lease INVISIBLE;
      ANALYZE TABLE media_encode_inbox;"

docker exec -i onfilm-worker-index-comparison \
  mysql -uroot -ponfilm_benchmark_password \
  < docs/performance/mysql-index-baseline-queries.sql
```

같은 Index를 다시 활성화한 뒤 동일하게 측정한다.

```bash
docker exec onfilm-worker-index-comparison \
  mysql -uroot -ponfilm_benchmark_password onfilm_worker \
  -e "ALTER TABLE media_encode_inbox
      ALTER INDEX idx_inbox_failure_pending VISIBLE,
      ALTER INDEX idx_inbox_status_lease VISIBLE;
      ANALYZE TABLE media_encode_inbox;"

docker exec -i onfilm-worker-index-comparison \
  mysql -uroot -ponfilm_benchmark_password \
  < docs/performance/mysql-index-baseline-queries.sql
```

각 조건의 첫 실행은 워밍업으로 제외하고 이후 세 실행의 중앙값을 기록한다. 측정이 끝나면 임시 컨테이너를 제거한다.

```bash
docker rm -f onfilm-worker-index-comparison
```

## 관련 문서

- [Worker 주요 SQL 추가 Index 적용 전 기준선](mysql-index-baseline.md)
- [Worker MySQL Testcontainers 환경](../WORKER_MYSQL_TESTCONTAINERS.md)
- [Worker MySQL Constraint 감사](../WORKER_MYSQL_CONSTRAINT_AUDIT.md)
- [Worker DB Schema 변경 규칙](../../AGENTS.md)
