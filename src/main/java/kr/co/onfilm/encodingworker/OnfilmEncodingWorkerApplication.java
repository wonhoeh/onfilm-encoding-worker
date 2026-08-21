package kr.co.onfilm.encodingworker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class OnfilmEncodingWorkerApplication {

	public static void main(String[] args) {
		SpringApplication.run(OnfilmEncodingWorkerApplication.class, args);
	}

}
