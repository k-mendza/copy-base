package copy.base.config;

import copy.base.domain.*;
import copy.base.util.JobCompletionNotificationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class BatchConfiguration {
    public static final int CHUNK_SIZE=10000;
    public static final int CORE_POOL_SIZE=4;
    public static final int MAX_CORE_POOL_SIZE=16;
    private static final Logger log = LoggerFactory.getLogger(BatchConfiguration.class);

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Bean
    public ColumnRangePartitioner partitioner(DataSource dataSource) {
        ColumnRangePartitioner columnRangePartitioner = new ColumnRangePartitioner();

        columnRangePartitioner.setColumn("id");
        columnRangePartitioner.setTable("client");
        columnRangePartitioner.setDataSource(dataSource);

        return columnRangePartitioner;
    }

    @Bean
    @StepScope
    public JdbcPagingItemReader<Client> pagingItemReader(
            DataSource dataSource,
            @Value("#{stepExecutionContext['minValue']}")Long minValue,
            @Value("#{stepExecutionContext['maxValue']}")Long maxValue) {
        log.info("reading " + minValue + " to " + maxValue);
        JdbcPagingItemReader<Client> reader = new JdbcPagingItemReader<>();

        reader.setDataSource(dataSource);
        reader.setFetchSize(1000);
        reader.setRowMapper(new ClientRowMapper());

        PostgresPagingQueryProvider queryProvider = new PostgresPagingQueryProvider();
        queryProvider.setSelectClause("id, firstName, lastName, email, phone");
        queryProvider.setFromClause("from client");
        queryProvider.setWhereClause("where id >= " + minValue + " and id <= " + maxValue);

        Map<String, Order> sortKeys = new HashMap<>(1);
        sortKeys.put("id", Order.ASCENDING);
        queryProvider.setSortKeys(sortKeys);
        reader.setQueryProvider(queryProvider);
        return reader;
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
    public JdbcBatchItemWriter<Client> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Client>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO client (id, firstName, lastName, email, phone) VALUES (:id, :firstName, :lastName, :email, :phone)")
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public Job importClientJob(JobCompletionNotificationListener listener, Step step1) {
        return jobBuilderFactory.get("importClientJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1)
                .end()
                .build();
    }

    @Bean
    public Step step1(JdbcBatchItemWriter<Client> writer) {
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
    public Step step2(JdbcBatchItemWriter<Client> writer, DataSource dataSource, Long minValue, Long maxValue) {
        return stepBuilderFactory.get("step2")
                .<Client, Client>chunk(CHUNK_SIZE)
                .reader(pagingItemReader(dataSource, minValue, maxValue))
                .processor(lowerCaseProcessor())
                .writer(writer)
                .build();
    }
}
