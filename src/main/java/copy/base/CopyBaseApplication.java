package copy.base;

import copy.base.domain.Client;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;

@EnableBatchProcessing
@SpringBootApplication
public class CopyBaseApplication {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    @StepScope
    public FlatFileItemReader<Client> fileClientReader(
            @Value("#{jobParameters['inputFlatFile']}") Resource resource) {

        return new FlatFileItemReaderBuilder<Client>()
                .saveState(false)
                .resource(resource)
                .delimited()
                .names(new String[] {"id", "firstName", "lastName", "email", "phone"})
                .fieldSetMapper(fieldSet -> {
                    Client client = new Client();

                    client.setId(fieldSet.readLong("id"));
                    client.setFirstName(fieldSet.readString("firstName"));
                    client.setLastName(fieldSet.readString("lastName"));
                    client.setEmail(fieldSet.readString("email"));
                    client.setPhone(fieldSet.readString("phone"));

                    return client;
                })
                .build();
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<Client> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Client>()
                .dataSource(dataSource)
                .beanMapped()
                .sql("INSERT INTO Client (ACCOUNT, AMOUNT, TIMESTAMP) VALUES (:account, :amount, :timestamp)")
                .build();
    }

    @Bean
    public Job multithreadedJob() {
        return this.jobBuilderFactory.get("multithreadedJob")
                .start(step1())
                .build();
    }

    @Bean
    public Step step1() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(4);
        taskExecutor.setMaxPoolSize(4);
        taskExecutor.afterPropertiesSet();

        return this.stepBuilderFactory.get("step1")
                .<Client, Client>chunk(100)
                .reader(fileClientReader(null))
                .writer(writer(null))
                .taskExecutor(taskExecutor)
                .build();
    }

    public static void main(String[] args) {
        String [] newArgs = new String[] {"inputFlatFile=./src/main/resources/clients_50k.csv"};

        SpringApplication.run(CopyBaseApplication.class, newArgs);
    }


}
