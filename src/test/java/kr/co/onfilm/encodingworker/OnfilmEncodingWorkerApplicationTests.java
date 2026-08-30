package kr.co.onfilm.encodingworker;

import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "spring.kafka.listener.auto-startup=false",
        "app.core-api.callback-secret=test-media-encode-callback-secret-32-bytes"
})
@ActiveProfiles("dev")
class OnfilmEncodingWorkerApplicationTests {

    @Autowired
    MeterRegistry meterRegistry;

    @Autowired
    Environment environment;

    @Test
    void contextLoads() {
        assertThat(meterRegistry.find("media.encode.worker.inbox.records").gauges())
                .isNotEmpty();
        assertThat(meterRegistry.find("media.encode.worker.inbox.records")
                .tag("application", "onfilm-encoding-worker")
                .tag("environment", "dev")
                .gauges()).isNotEmpty();
        assertThat(meterRegistry.find(
                "media.encode.worker.inbox.oldest.failure.pending.age").gauge())
                .isNotNull();
        assertThat(environment.getProperty("server.port")).isEqualTo("8082");
    }
}
