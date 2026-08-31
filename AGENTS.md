# OnFilm Encoding Worker agent work rules

This file applies to the entire OnFilm Encoding Worker repository.

## Database schema changes

### Database ownership

- This repository owns only the `onfilm_worker` logical database and its Flyway migrations.
- The Worker currently owns `media_encode_inbox`, including message idempotency, lease, retry, Callback, and terminal-state persistence.
- The OnFilm API repository owns `onfilm_api`, including `media_upload_requests`, `media_encode_jobs`, and `media_encode_outbox`.
- Do not add cross-database foreign keys, joins, views, repositories, or transactions between Worker and API databases.
- `media_encode_inbox.job_id` correlates with the API Job through Kafka messages and Callback APIs; it is not a database foreign key.
- Do not read or update the API database directly. Exchange data only through the versioned Kafka message and authenticated Callback API contracts.
- Do not create databases or grant users in application Flyway migrations. Infrastructure initialization owns database and account provisioning.

### Schema source of truth

- Flyway versioned SQL migrations are the schema source of truth after the initial `V1` migration is introduced.
- Hibernate must validate mappings with `ddl-auto: validate`; it must not create, update, or drop shared schemas.
- H2 is not evidence that a schema change works on MySQL. Verify persistence behavior on the MySQL Testcontainers environment.
- Until `V1` is introduced, include every new mapping change in the pending Worker `V1` design and do not add new reliance on `create`, `create-drop`, or `update`.
- Do not enable Flyway `baselineOnMigrate`. The initial migration targets an empty `onfilm_worker` database because there is no production data to preserve.

### Changes that require migration review

Treat a change as a database schema change when it affects any of the following:

- Entity, table, column, identifier, or generation strategy
- SQL type, length, precision, scale, nullability, default value, or enum representation
- Unique, check, foreign-key, or index definition
- Optimistic-lock version column
- Inbox status, lease, retry, payload, failure, or timestamp persistence
- Native SQL or a repository query that depends on a new index
- Persisted data transformation

Pure Java validation or processing changes do not require a migration only when the persisted representation remains unchanged. State that reasoning in the handoff when it may not be obvious.

### Migration rules

- Store Worker migrations under `src/main/resources/db/migration`.
- Name versioned migrations `V<version>__<snake_case_description>.sql`.
- Use a new, monotonically increasing version for every later change.
- Never edit or rename a committed migration that may have been applied. Add a corrective migration instead.
- Keep Worker and API version numbers independent; never place API DDL in this repository.
- Write MySQL-compatible SQL and test it on the project-pinned MySQL version.
- Do not qualify table names with `onfilm_worker.` inside migrations. The configured datasource selects the target logical database.
- Make column nullability and important defaults explicit instead of relying on implicit database behavior.
- Give constraints and indexes stable names using `uk_`, `fk_`, `ck_`, and `idx_` prefixes.
- Add an index only for a demonstrated claim, stale-recovery, Callback, retention, or operational query. Record the query and compare `EXPLAIN` before and after performance indexes.
- Do not introduce a foreign key to an API-owned identifier.

### Inbox invariants

- Preserve `job_id` as the Worker idempotency key and reject the same `job_id` with a different message identity or payload.
- Preserve optimistic locking when a change can race with duplicate delivery, stale recovery, or Callback-only recovery.
- Review valid status transitions before changing the stored `InboxStatus` representation.
- Treat terminal `DONE` and `FAILED` states as irreversible unless a separately approved recovery policy says otherwise.
- Keep lease and retry queries consistent with their composite-index column order.
- Consider existing persisted payloads and enum strings before renaming message fields, statuses, presets, job types, or failure codes.
- Do not use a database transaction to cover ffmpeg, S3, Kafka polling, or API Callback I/O. Persist the state needed to retry instead.

### Entity and migration consistency

- Change the JPA mapping and its Flyway migration in the same work unit.
- Keep column names, lengths, nullability, enum storage, constraints, and indexes consistent between `MediaEncodeInbox` and SQL.
- Keep repository claim and recovery queries consistent with the schema and indexes.
- Use a Unique Constraint or primary key for invariants that must remain true under concurrent message delivery; an application-level existence check alone is insufficient.

### Fixtures and persisted test data

- Keep local and test fixtures out of production migrations.
- Do not store credentials, Callback secrets, real storage keys, source media metadata, or production Kafka payloads in migrations.
- Tests must create their own Inbox rows and must not depend on a developer's local file-based H2 database.

### Required verification

For a schema change, verify the applicable items before reporting completion:

1. Apply all Worker migrations from an empty MySQL database.
2. Start the Spring context with Hibernate `ddl-auto: validate`.
3. Run the affected repository and transaction tests on MySQL Testcontainers.
4. Add a rejection test for new Unique, Check, Not Null, or foreign-key constraints.
5. Re-run duplicate delivery, lease expiration, stale recovery, Callback-only recovery, and terminal-state tests when their columns or queries change.
6. Add a concurrent-processing test when correctness relies on uniqueness, optimistic locking, or claim locking.
7. Run `./gradlew test` and `./gradlew integrationTest`; use `./gradlew check` for final repository-wide verification.
8. For index changes, capture comparable `EXPLAIN` or `EXPLAIN ANALYZE` evidence using the same query and dataset.

If the MySQL Testcontainers or Flyway environment required by these checks has not been introduced yet, report that limitation explicitly instead of treating an H2 result as final verification.

### Safety and documentation

- Never store database passwords or real connection strings in Git. Use `WORKER_DB_URL`, `WORKER_DB_USER`, and `WORKER_DB_PASSWORD` through environment variables or secrets.
- Do not assume that a database remains disposable merely because it currently has no production data. Confirm the target environment before a destructive migration or reset.
- Separate destructive or data-rewriting migrations from unrelated Worker changes and document backup, verification, and recovery steps.
- Document non-obvious status constraints, nullable lease fields, retention indexes, and composite-index column order.
- Update `docs/WORKER_RELIABILITY_DECISIONS.md` when a schema change alters Inbox idempotency, lease recovery, retry, Callback, or terminal-state behavior.
