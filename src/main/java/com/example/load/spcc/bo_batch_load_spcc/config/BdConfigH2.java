package com.example.load.spcc.bo_batch_load_spcc.config;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

@Configuration
public class BdConfigH2 {

    @Bean
    @Primary
    @ConfigurationProperties("spring.datasources.h2")
    public DataSourceProperties h2DataSourceProperties() {
        return new DataSourceProperties();
    }

    @Primary
    @Bean(name = "dataSource")
    @ConfigurationProperties(prefix = "spring.datasources.h2")
    public DataSource dataSource() {
        return h2DataSourceProperties()
                .initializeDataSourceBuilder()
                .build();
    }
}
