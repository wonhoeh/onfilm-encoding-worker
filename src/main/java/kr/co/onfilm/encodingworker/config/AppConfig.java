package kr.co.onfilm.encodingworker.config;

<<<<<<< HEAD
import org.springframework.boot.context.properties.EnableConfigurationProperties;
=======
import kr.co.onfilm.encodingworker.infra.storage.LocalStorageClient;
import kr.co.onfilm.encodingworker.infra.storage.S3StorageClient;
import kr.co.onfilm.encodingworker.infra.storage.StorageClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.beans.factory.ObjectProvider;
>>>>>>> a4d4e61 (feat: 로컬에서 인코딩 테스트할 수 있는 환경 구성 및 문서 작업)
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.web.client.RestClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Configuration
@EnableConfigurationProperties(AppProperties.class)
public class AppConfig {

    @Bean
<<<<<<< HEAD
=======
    @ConditionalOnProperty(prefix = "app.storage", name = "type", havingValue = "s3")
>>>>>>> a4d4e61 (feat: 로컬에서 인코딩 테스트할 수 있는 환경 구성 및 문서 작업)
    S3Client s3Client(AppProperties properties) {
        return S3Client.builder()
                .region(Region.of(properties.storage().region()))
                .build();
    }

    @Bean
<<<<<<< HEAD
=======
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
>>>>>>> a4d4e61 (feat: 로컬에서 인코딩 테스트할 수 있는 환경 구성 및 문서 작업)
    RestClient restClient(AppProperties properties) {
        return RestClient.builder()
                .baseUrl(properties.coreApi().baseUrl().toString())
                .defaultHeader("Authorization", "Bearer " + properties.coreApi().authToken())
                .build();
    }

    @Bean
    RecordMessageConverter recordMessageConverter() {
        return new StringJsonMessageConverter();
    }
}
