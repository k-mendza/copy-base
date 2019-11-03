package copy.base.config;

import copy.base.domain.Client;
import copy.base.domain.ClientItemProcessor;
import copy.base.util.JobCompletionNotificationListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Bean
    public FlatFileItemReader<Client> reader() {
        return new FlatFileItemReaderBuilder<Client>()
                .name("clientItemReader")
                .resource(new ClassPathResource("clients_50k.csv"))
                .delimited()
                .names(new String[]{"id", "firstName", "lastName", "email", "phone"})
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
    public ClientItemProcessor processor() {
        return new ClientItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Client> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Client>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO CLIENT (id, firstName, lastName, email, phone) VALUES (:id, :firstName, :lastName, :email, :phone)")
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public Job importUserJob(JobCompletionNotificationListener listener, Step step1) {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1)
                .end()
                .build();
    }

    @Bean
    public Step step1(JdbcBatchItemWriter<Client> writer) {
        return stepBuilderFactory.get("step1")
                .<Client, Client> chunk(1000)
                .reader(reader())
                .processor(processor())
                .writer(writer)
                .build();
    }
}
