package copy.base.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(
        transactionManagerRef = "datatargetTransactionManager",
        entityManagerFactoryRef = "datatargetEntityManagerFactory",
        basePackages = {"copy.base.domain.datatarget"}
)
public class DataTargetConfiguration {

    @Bean(name = "datatargetProperties")
    @ConfigurationProperties("postgres.datatarget")
    public DataSourceProperties datatargetProperties() {
        return new DataSourceProperties();
    }

    @Bean(name = "datatarget")
    public DataSource datatarget(@Qualifier("datatargetProperties") DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder().build();
    }

    @Primary
    @Bean(name = "datatargetEntityManagerFactory")
    public LocalContainerEntityManagerFactoryBean datatargetEntityManagerFactoryBean(EntityManagerFactoryBuilder builder, @Qualifier("datatarget") DataSource datatarget) {
        return builder.dataSource(datatarget)
                .packages("copy.base.domain.datatarget")
                .persistenceUnit("datatarget")
                .build();
    }

    @Primary
    @Bean(name = "datatargetTransactionManager")
    @ConfigurationProperties("postgres.jpa")
    public PlatformTransactionManager transactionManager(@Qualifier("datatargetEntityManagerFactory") EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }
}
