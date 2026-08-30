package kr.co.onfilm.encodingworker;

import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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

    @Test
    void contextLoads() {
        assertThat(meterRegistry.find("media.encode.worker.inbox.records").gauges())
                .isNotEmpty();
        assertThat(meterRegistry.find(
                "media.encode.worker.inbox.oldest.failure.pending.age").gauge())
                .isNotNull();
    }
}
