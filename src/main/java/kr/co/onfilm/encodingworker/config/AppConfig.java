package kr.co.onfilm.encodingworker.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
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
    S3Client s3Client(AppProperties properties) {
        return S3Client.builder()
                .region(Region.of(properties.storage().region()))
                .build();
    }

    @Bean
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
