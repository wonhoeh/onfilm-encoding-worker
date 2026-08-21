package kr.co.onfilm.encodingworker.config;

import kr.co.onfilm.encodingworker.infra.storage.LocalStorageClient;
import kr.co.onfilm.encodingworker.infra.storage.S3StorageClient;
import kr.co.onfilm.encodingworker.infra.storage.StorageClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.web.client.RestClient;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import java.net.http.HttpClient;
import kr.co.onfilm.encodingworker.infra.coreapi.InternalCallbackHmacInterceptor;
import java.time.Clock;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;

@Configuration
@EnableConfigurationProperties(AppProperties.class)
public class AppConfig {

    @Bean
    @ConditionalOnProperty(prefix = "app.storage", name = "type", havingValue = "s3")
    S3Client s3Client(AppProperties properties) {
        return S3Client.builder()
                .region(Region.of(properties.storage().region()))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallTimeout(properties.storage().apiCallTimeout())
                        .apiCallAttemptTimeout(properties.storage().apiCallAttemptTimeout())
                        .build())
                .build();
    }

    @Bean
    StorageClient storageClient(
            AppProperties properties,
            ObjectProvider<LocalStorageClient> localStorageClientProvider,
            ObjectProvider<S3StorageClient> s3StorageClientProvider
    ) {
        return switch (properties.storage().type().toLowerCase()) {
            case "local" -> requireBean(localStorageClientProvider.getIfAvailable(), "local");
            case "s3" -> requireBean(s3StorageClientProvider.getIfAvailable(), "s3");
            default -> throw new IllegalStateException("Unsupported app.storage.type: " + properties.storage().type());
        };
    }

    private StorageClient requireBean(StorageClient storageClient, String type) {
        if (storageClient == null) {
            throw new IllegalStateException("Storage client bean not available for app.storage.type=" + type);
        }
        return storageClient;
    }

    @Bean
    RestClient restClient(AppProperties properties, InternalCallbackHmacInterceptor hmacInterceptor) {
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(properties.coreApi().connectTimeout())
                .build();
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(properties.coreApi().readTimeout());
        return RestClient.builder()
                .baseUrl(properties.coreApi().baseUrl().toString())
                .requestFactory(requestFactory)
                .requestInterceptor(hmacInterceptor)
                .build();
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    RecordMessageConverter recordMessageConverter() {
        return new StringJsonMessageConverter();
    }
}
