package kr.co.onfilm.encodingworker.support;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

public abstract class MySqlContainerSupport {

    private static final DockerImageName MYSQL_IMAGE =
            DockerImageName.parse("mysql:8.4.11");

    protected static final MySQLContainer<?> MYSQL = new MySQLContainer<>(MYSQL_IMAGE)
            .withDatabaseName("onfilm_worker")
            .withUsername("onfilm_worker_app")
            .withPassword("onfilm_worker_test_password")
            .withCommand(
                    "--character-set-server=utf8mb4",
                    "--collation-server=utf8mb4_0900_ai_ci"
            )
            .withStartupTimeout(Duration.ofMinutes(3));

    static {
        MYSQL.start();
    }

    @DynamicPropertySource
    static void configureMySql(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("spring.datasource.driver-class-name", MYSQL::getDriverClassName);
        registry.add("spring.jpa.database-platform", () -> "org.hibernate.dialect.MySQLDialect");

        registry.add("spring.flyway.enabled", () -> "true");
        registry.add("spring.flyway.baseline-on-migrate", () -> "false");
        registry.add("spring.flyway.validate-migration-naming", () -> "true");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "validate");
        registry.add("spring.jpa.defer-datasource-initialization", () -> "false");
        registry.add("spring.sql.init.mode", () -> "never");
    }
}
