# Worker MySQL Testcontainers 통합 테스트 환경

- 적용일: 2026-09-02
- 적용 범위: Encoding Worker `integrationTest` 소스셋
- 이미지: `mysql:8.4.11`

## 목적

H2는 Worker의 빠른 단위·슬라이스 테스트에 유용하지만 MySQL enum, UUID 저장 방식, Collation, InnoDB Transaction과 Lock을 동일하게 재현하지 않는다. DB 동작이 결과에 영향을 주는 통합 테스트는 Worker가 실제로 사용할 MySQL과 같은 버전에서 실행한다.

## 구성

| 항목 | 값 |
|---|---|
| 논리 DB | `onfilm_worker` |
| 계정 | `onfilm_worker_app` |
| 문자 집합 | `utf8mb4` |
| Collation | `utf8mb4_0900_ai_ci` |
| Testcontainers | `1.21.4` |
| 공통 지원 클래스 | `MySqlContainerSupport` |

Testcontainers가 통합 테스트 JVM에서 전용 MySQL을 시작하고 JDBC URL, 계정과 비밀번호를 Spring에 동적으로 전달한다. 고정 호스트 포트를 사용하지 않으며 로컬 Compose DB와 데이터를 공유하지 않는다.

`@DataJpaTest`가 Datasource를 H2로 교체하지 않도록 MySQL 통합 테스트는 다음 설정을 사용한다.

```java
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class ExampleIntegrationTest extends MySqlContainerSupport {
}
```

통합 테스트의 컨테이너 공유와 DB 상태 충돌을 예측 가능하게 유지하기 위해 `integrationTest`의 `maxParallelForks`는 1로 설정한다. 각 테스트는 자신이 만든 Inbox 데이터를 직접 정리한다.

## 현재 검증 범위

- MySQL 버전이 `8.4.11`인지 확인
- 연결 DB가 Worker 소유 `onfilm_worker`인지 확인
- 전용 계정이 `onfilm_worker_app`인지 확인
- 문자 집합과 Collation 확인
- `media_encode_inbox` JPA Schema가 실제 MySQL에 생성되는지 확인
- 빈 DB에서 Flyway V1 적용과 `flyway_schema_history` 확인
- Hibernate `ddl-auto: validate`로 Entity·Schema 일치 확인
- `job_id`가 Kafka·Callback 계약과 같은 `VARCHAR(36)`인지 확인
- Inbox `payload`가 255바이트 제한의 `TINYTEXT`가 아닌 `TEXT`인지 확인
- claim·복구용 Composite Index의 존재와 컬럼 순서 확인
- 기존 Inbox 중복 메시지, lease 만료, Callback-only와 terminal 상태 장애 주입 테스트를 MySQL에서 실행

최초 MySQL 실행에서는 `@Lob`만 선언한 `payload`가 `TINYTEXT`로 생성되어 실제 Kafka JSON 저장이 `Data too long for column 'payload'`로 실패했다. Entity Mapping에 `columnDefinition = "TEXT"`를 명시해 H2에서 발견하지 못했던 차이를 수정했고, 다음 V1도 같은 타입으로 작성한다.

UUID 타입의 `job_id`는 `length = 36`만으로는 MySQL에서 `BINARY(36)`으로 생성됐다. 16바이트 UUID가 고정 길이로 패딩되면서 같은 ID의 잠금 조회가 행을 찾지 못하고 이어진 INSERT가 PK 중복으로 실패했다. `@JdbcTypeCode(SqlTypes.VARCHAR)`를 추가해 `VARCHAR(36)` 저장과 조회를 일치시켰으며 다음 V1에서도 이 표현을 유지한다.

## Flyway와 Hibernate 역할

Worker MySQL Schema의 단일 기준은 `V1__create_initial_schema.sql`부터 시작하는 Flyway Versioned Migration이다. Hibernate는 공유 MySQL Schema를 생성하거나 변경하지 않고 `ddl-auto: validate`로 Entity Mapping과의 불일치만 탐지한다.

통합 테스트는 다음 순서로 실행된다.

1. Testcontainers가 빈 `onfilm_worker`를 시작한다.
2. Flyway가 V1을 적용한다.
3. Hibernate가 전체 Entity Mapping을 validate한다.
4. Environment Test가 Migration 버전, 컬럼 타입과 Index 순서를 확인한다.
5. Inbox 영속성·장애 시나리오를 실행한다.

`baselineOnMigrate`는 사용하지 않는다. 보존할 운영 데이터가 없는 정책에 따라 빈 DB에서 V1부터 재현한다. 개발 프로필의 메모리 H2는 빠른 로컬 확인을 위해 Flyway를 비활성화하고 Hibernate `create-drop`을 사용하지만, Schema 호환성의 증거로 사용하지 않는다.

## 실행

Docker Engine이 실행 중인 Worker 저장소에서 다음 명령을 사용한다.

```bash
./gradlew integrationTest
```

단위 테스트와 MySQL 통합 테스트를 모두 실행하려면 다음 명령을 사용한다.

```bash
./gradlew check
```

Docker에 접근할 수 없으면 MySQL 통합 테스트를 성공으로 건너뛰지 않고 실패시킨다.

## 관련 문서

- [Worker 신뢰성 결정](WORKER_RELIABILITY_DECISIONS.md)
- [Worker DB Schema 변경 규칙](../AGENTS.md)
