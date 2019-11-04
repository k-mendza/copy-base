package copy.base;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableBatchProcessing
@SpringBootApplication
public class CopyBaseApplication {

    public static void main(String[] args) {
        SpringApplication.run(CopyBaseApplication.class, args);
    }
}
