package kr.co.onfilm.encodingworker.infra.coreapi;

import kr.co.onfilm.encodingworker.config.AppProperties;
import kr.co.onfilm.encodingworker.observability.CorrelationIdContext;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.*;
import org.springframework.stereotype.Component;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.util.HexFormat;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class InternalCallbackHmacInterceptor implements ClientHttpRequestInterceptor {
    private final AppProperties properties;
    private final Clock clock;

    @Override
    public ClientHttpResponse intercept(HttpRequest request, byte[] body,
                                        ClientHttpRequestExecution execution) throws IOException {
        long timestamp = clock.instant().getEpochSecond();
        String nonce = UUID.randomUUID().toString();
        String canonical = timestamp + "\n" + nonce + "\n"
                + request.getMethod().name() + "\n"
                + request.getURI().getRawPath() + "\n"
                + sha256(body);
        request.getHeaders().set("X-Onfilm-Timestamp", Long.toString(timestamp));
        request.getHeaders().set("X-Onfilm-Nonce", nonce);
        request.getHeaders().set("X-Onfilm-Signature", hmac(canonical));
        request.getHeaders().set(
                CorrelationIdContext.HEADER_NAME,
                CorrelationIdContext.currentOrCreate()
        );
        return execution.execute(request, body);
    }

    private String sha256(byte[] body) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body));
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to calculate request body hash", exception);
        }
    }

    private String hmac(String canonical) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(
                    properties.coreApi().callbackSecret().getBytes(StandardCharsets.UTF_8),
                    "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal(canonical.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to sign internal callback", exception);
        }
    }
}
