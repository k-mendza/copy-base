package copy.base.config;

import copy.base.domain.datasource.*;
import copy.base.util.JobCompletionNotificationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;

@Configuration
public class BatchConfiguration {
    public static final int CHUNK_SIZE = 2048;
    public static final int CORE_POOL_SIZE = 4;
    public static final int MAX_CORE_POOL_SIZE = 16;
    private static final Logger log = LoggerFactory.getLogger(BatchConfiguration.class);

    public final JobBuilderFactory jobBuilderFactory;
    public final StepBuilderFactory stepBuilderFactory;
    public final DataSource dataSource;
    public final DataSource dataTarget;

    public BatchConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, @Qualifier("datasource") DataSource dataSource, @Qualifier("datatarget") DataSource datatarget) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.dataSource = dataSource;
        this.dataTarget = datatarget;
    }

    @Bean
    public ColumnRangePartitioner partitioner() {
        ColumnRangePartitioner columnRangePartitioner = new ColumnRangePartitioner();

        columnRangePartitioner.setColumn("id");
        columnRangePartitioner.setTable("client");
        columnRangePartitioner.setDataSource(dataSource);

        return columnRangePartitioner;
    }

    @Bean
    public JdbcCursorItemReader<Client> cursorItemReader() {
        return new JdbcCursorItemReaderBuilder<Client>()
                .dataSource(this.dataSource)
                .name("clientReader")
                .sql("SELECT * FROM CLIENT")
                .rowMapper(new ClientRowMapper())
                .build();
    }

    @Bean
    public FlatFileItemReader<Client> fileReader() {
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
                }).build();
    }

    @Bean
    public ClientUpperCaseProcessor upperCaseProcessor() {
        return new ClientUpperCaseProcessor();
    }

    @Bean
    public ClientLowerCaseProcessor lowerCaseProcessor() {
        return new ClientLowerCaseProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Client> dataSourceWriter() {
        return new JdbcBatchItemWriterBuilder<Client>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO client (id, firstName, lastName, email, phone) VALUES (:id, :firstName, :lastName, :email, :phone)")
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<Client> dataTargetWriter() {
        return new JdbcBatchItemWriterBuilder<Client>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO client (id, firstName, lastName, email, phone) VALUES (:id, :firstName, :lastName, :email, :phone)")
                .dataSource(dataTarget)
                .build();
    }

    @Bean
    public Job importClientJob(JobCompletionNotificationListener listener, Step step1, Step step2) {
        return jobBuilderFactory.get("importClientJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1)
                .next(step2)
                .end()
                .build();
    }

    @Bean
    public Step step1(@Qualifier("dataSourceWriter") JdbcBatchItemWriter<Client> writer) {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(CORE_POOL_SIZE);
        taskExecutor.setMaxPoolSize(MAX_CORE_POOL_SIZE);
        taskExecutor.afterPropertiesSet();

        return stepBuilderFactory.get("step1")
                .<Client, Client>chunk(CHUNK_SIZE)
                .reader(fileReader())
                .processor(upperCaseProcessor())
                .writer(writer)
                .taskExecutor(taskExecutor)
                .build();
    }

    @Bean
    public Step step2(@Qualifier("dataTargetWriter") JdbcBatchItemWriter<Client> writer) {
        return stepBuilderFactory.get("step2")
                .<Client, Client>chunk(CHUNK_SIZE)
                .reader(cursorItemReader())
                .processor(lowerCaseProcessor())
                .writer(writer)
                .build();
    }
}
