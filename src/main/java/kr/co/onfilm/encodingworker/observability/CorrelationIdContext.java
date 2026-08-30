package kr.co.onfilm.encodingworker.observability;

import org.slf4j.MDC;

import java.util.UUID;
import java.util.regex.Pattern;

public final class CorrelationIdContext {
    public static final String HEADER_NAME = "X-Correlation-Id";
    public static final String MDC_KEY = "correlationId";
    public static final int MAX_LENGTH = 64;

    private static final Pattern SAFE_VALUE = Pattern.compile("^[A-Za-z0-9._-]{1," + MAX_LENGTH + "}$");

    private CorrelationIdContext() {
    }

    public static String resolve(String candidate, UUID fallback) {
        if (isValid(candidate)) {
            return candidate;
        }
        return fallback == null ? UUID.randomUUID().toString() : fallback.toString();
    }

    public static String currentOrCreate() {
        return resolve(MDC.get(MDC_KEY), null);
    }

    public static boolean isValid(String value) {
        return value != null && SAFE_VALUE.matcher(value).matches();
    }
}
