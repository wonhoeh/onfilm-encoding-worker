package kr.co.onfilm.encodingworker.infra.coreapi;

import kr.co.onfilm.encodingworker.TestProperties;
import org.junit.jupiter.api.Test;
import org.springframework.http.*;
import org.springframework.http.client.*;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.*;
import java.util.HexFormat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class InternalCallbackHmacInterceptorTest {
    private static final Instant NOW = Instant.parse("2026-08-21T00:00:00Z");

    @Test
    void signsMethodPathTimestampNonceAndActualBody() throws Exception {
        InternalCallbackHmacInterceptor interceptor = new InternalCallbackHmacInterceptor(
                TestProperties.create(), Clock.fixed(NOW, ZoneOffset.UTC));
        HttpRequest request = mock(HttpRequest.class);
        HttpHeaders headers = new HttpHeaders();
        when(request.getHeaders()).thenReturn(headers);
        when(request.getMethod()).thenReturn(HttpMethod.POST);
        when(request.getURI()).thenReturn(
                URI.create("http://localhost/internal/api/media-jobs/job/complete?ignored=true"));
        ClientHttpRequestExecution execution = mock(ClientHttpRequestExecution.class);
        ClientHttpResponse response = mock(ClientHttpResponse.class);
        byte[] body = "{\"completedAt\":\"2026-08-21T00:00:00Z\"}".getBytes(StandardCharsets.UTF_8);
        when(execution.execute(request, body)).thenReturn(response);

        assertThat(interceptor.intercept(request, body, execution)).isSameAs(response);

        String timestamp = headers.getFirst("X-Onfilm-Timestamp");
        String nonce = headers.getFirst("X-Onfilm-Nonce");
        String canonical = timestamp + "\n" + nonce + "\nPOST\n"
                + "/internal/api/media-jobs/job/complete\n" + sha256(body);
        assertThat(timestamp).isEqualTo(Long.toString(NOW.getEpochSecond()));
        assertThat(headers.getFirst("X-Onfilm-Signature")).isEqualTo(hmac(canonical));
    }

    private String sha256(byte[] body) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body));
    }

    private String hmac(String canonical) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(TestProperties.SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return HexFormat.of().formatHex(mac.doFinal(canonical.getBytes(StandardCharsets.UTF_8)));
    }
}
