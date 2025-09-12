package com.example.load.spcc.bo_batch_load_spcc.config;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;

@Configuration
@Slf4j
public class BdConfigSqlServer {


    private final Environment env;

    String username = "";
    String password = "";

    public BdConfigSqlServer(Environment env) {
        this.env = env;
    }


    @Bean(name = "target")
    public DataSource targetconfDataSource() {

        username = env.getProperty("spring.datasources.target.username");
        password = env.getProperty("spring.datasources.target.password");


        return this.createDataSource(username, password);
    }

    @Bean(name = "jdbcTemplate")
    public JdbcTemplate dataSourceJdbcTemplate(@Qualifier("target") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean(name = "jdbcTemplateInsert")
    public NamedParameterJdbcTemplate dataSourceJdbcTemplateInsertr(@Qualifier("target") DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    private HikariDataSource createDataSource(String username, String password) {
        final HikariConfig dataSourceConfig = new HikariConfig();

        dataSourceConfig.setJdbcUrl(env.getProperty("spring.datasources.target.jdbc-url"));
        dataSourceConfig.setUsername(username);
        dataSourceConfig.setPassword(password);
        dataSourceConfig.setDriverClassName(env.getProperty("spring.datasources.target.driver-class-name"));
        dataSourceConfig.setPoolName(env.getProperty("spring.datasources.target.poolName"));
        dataSourceConfig.setMaximumPoolSize(150);
        dataSourceConfig.setMinimumIdle(2);
        dataSourceConfig.setConnectionTimeout(300000);
        dataSourceConfig.setIdleTimeout(180000);
        dataSourceConfig.addDataSourceProperty("cachePrepStmts", env.getProperty("spring.datasources.target.properties.cachePrepStmts", Boolean.class));
        dataSourceConfig.addDataSourceProperty("prepStmtCacheSize", env.getProperty("spring.datasources.target.properties.prepStmtCacheSize", Integer.class));
        dataSourceConfig.addDataSourceProperty("prepStmtCacheSqlLimit", env.getProperty("spring.datasources.target.properties.prepStmtCacheSqlLimit", Integer.class));
        dataSourceConfig.addDataSourceProperty("useServerPrepStmts", env.getProperty("spring.datasources.target.properties.useServerPrepStmts", Boolean.class));

        dataSourceConfig.validate();

        return new HikariDataSource(dataSourceConfig);
    }
}
