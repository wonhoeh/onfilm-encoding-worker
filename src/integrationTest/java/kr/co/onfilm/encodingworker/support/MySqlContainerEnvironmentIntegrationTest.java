package kr.co.onfilm.encodingworker.support;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest(properties = "spring.sql.init.mode=never")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class MySqlContainerEnvironmentIntegrationTest extends MySqlContainerSupport {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void integrationTestsUsePinnedMySqlAndWorkerDatabase() {
        String database = jdbcTemplate.queryForObject("SELECT DATABASE()", String.class);
        String currentUser = jdbcTemplate.queryForObject("SELECT CURRENT_USER()", String.class);
        String version = jdbcTemplate.queryForObject("SELECT VERSION()", String.class);
        String characterSet = jdbcTemplate.queryForObject(
                "SELECT @@character_set_database",
                String.class
        );
        String collation = jdbcTemplate.queryForObject(
                "SELECT @@collation_database",
                String.class
        );
        Integer inboxTableCount = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                  AND table_name = 'media_encode_inbox'
                """, Integer.class);
        String payloadDataType = jdbcTemplate.queryForObject("""
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                  AND table_name = 'media_encode_inbox'
                  AND column_name = 'payload'
                """, String.class);
        String jobIdDataType = jdbcTemplate.queryForObject("""
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                  AND table_name = 'media_encode_inbox'
                  AND column_name = 'job_id'
                """, String.class);
        Long jobIdMaxLength = jdbcTemplate.queryForObject("""
                SELECT character_maximum_length
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                  AND table_name = 'media_encode_inbox'
                  AND column_name = 'job_id'
                """, Long.class);
        String migrationVersion = jdbcTemplate.queryForObject("""
                SELECT version
                FROM flyway_schema_history
                WHERE success = TRUE
                ORDER BY installed_rank DESC
                LIMIT 1
                """, String.class);

        assertThat(database).isEqualTo("onfilm_worker");
        assertThat(currentUser).startsWith("onfilm_worker_app@");
        assertThat(version).startsWith("8.4.11");
        assertThat(characterSet).isEqualTo("utf8mb4");
        assertThat(collation).isEqualTo("utf8mb4_0900_ai_ci");
        assertThat(inboxTableCount).isEqualTo(1);
        assertThat(payloadDataType).isEqualTo("text");
        assertThat(jobIdDataType).isEqualTo("varchar");
        assertThat(jobIdMaxLength).isEqualTo(36L);
        assertThat(migrationVersion).isEqualTo("2");
        assertIndexColumns("idx_inbox_status_lease", "status", "lease_until");
        assertIndexColumns("idx_inbox_failure_pending", "status", "updated_at");
        assertCheckConstraints();
    }

    private void assertIndexColumns(String indexName, String... expectedColumns) {
        List<String> columns = jdbcTemplate.queryForList("""
                SELECT column_name
                FROM information_schema.statistics
                WHERE table_schema = DATABASE()
                  AND table_name = 'media_encode_inbox'
                  AND index_name = ?
                ORDER BY seq_in_index
                """, String.class, indexName);

        assertThat(columns).containsExactly(expectedColumns);
    }

    private void assertCheckConstraints() {
        List<String> constraints = jdbcTemplate.queryForList("""
                SELECT constraint_name
                FROM information_schema.table_constraints
                WHERE constraint_schema = DATABASE()
                  AND table_name = 'media_encode_inbox'
                  AND constraint_type = 'CHECK'
                """, String.class);

        assertThat(constraints).containsExactlyInAnyOrder(
                "ck_inbox_attempts_positive",
                "ck_inbox_version_non_negative",
                "ck_inbox_timestamp_order",
                "ck_inbox_lease_after_update",
                "ck_inbox_failure_pair",
                "ck_inbox_failure_reason_not_blank",
                "ck_inbox_lease_status",
                "ck_inbox_failure_status",
                "ck_inbox_done_clears_failure",
                "ck_inbox_payload_json"
        );
    }
}
