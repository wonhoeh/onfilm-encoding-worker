package kr.co.onfilm.encodingworker;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(properties = {
        "spring.kafka.listener.auto-startup=false",
        "app.core-api.callback-secret=test-media-encode-callback-secret-32-bytes"
})
@ActiveProfiles("dev")
class OnfilmEncodingWorkerApplicationTests {

    @Test
    void contextLoads() {
    }
}
