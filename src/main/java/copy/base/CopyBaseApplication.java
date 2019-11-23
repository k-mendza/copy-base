package copy.base;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableBatchProcessing
@SpringBootApplication
public class CopyBaseApplication {

    // TODO 1 automatic shutdown after job completion
    // TODO 2 accept database connection information: host, port, database name, user, password from command line
    // TODO 4 configure app to run as jar (pay attention to include dependencies and correctly find file inside jar)
    // TODO 5 change data source from csv to database
    // TODO 6 add more tables to copy
    // TODO 7 modify processors for selected tables

    public static void main(String[] args) {
        SpringApplication.run(CopyBaseApplication.class, args);
    }
}
