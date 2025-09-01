package com.example.load.spcc.bo_batch_load_spcc.config;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;

@Configuration
@Slf4j
public class BdConfigInfx {


    @Autowired
    private Environment env;

    String username = "";
    String password = "";


    @Bean(name = "targetInfx")
    public DataSource targetconfDataSource() {

        username = env.getProperty("spring.datasources.informix.origin.username");
        password = env.getProperty("spring.datasources.informix.origin.password");
        return this.createDataSource(username, password);
    }

    @Bean(name = "jdbcTemplateInfx")
    public JdbcTemplate dataSourceJdbcTemplate(@Qualifier("targetInfx") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean(name = "jdbcTemplateInsertInfx")
    public NamedParameterJdbcTemplate dataSourceJdbcTemplateInsertr(@Qualifier("targetInfx") DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    private HikariDataSource createDataSource(String username, String password) {
        final HikariConfig dataSourceConfig = new HikariConfig();

        dataSourceConfig.setJdbcUrl(env.getProperty("spring.datasources.informix.origin.jdbc-url"));
        dataSourceConfig.setUsername(username);
        dataSourceConfig.setPassword(password);
        dataSourceConfig.setDriverClassName(env.getProperty("spring.datasources.informix.origin.driver-class-name"));
        dataSourceConfig.setPoolName(env.getProperty("spring.datasources.informix.origin.poolName"));
        dataSourceConfig.setMaximumPoolSize(150);
        dataSourceConfig.setMinimumIdle(2);
        dataSourceConfig.setConnectionTimeout(300000);
        dataSourceConfig.setIdleTimeout(180000);
        dataSourceConfig.addDataSourceProperty("cachePrepStmts", env.getProperty("spring.datasources.informix.origin.properties.cachePrepStmts", Boolean.class));
        dataSourceConfig.addDataSourceProperty("prepStmtCacheSize", env.getProperty("spring.datasources.informix.origin.properties.prepStmtCacheSize", Integer.class));
        dataSourceConfig.addDataSourceProperty("prepStmtCacheSqlLimit", env.getProperty("spring.datasources.informix.origin.properties.prepStmtCacheSqlLimit", Integer.class));
        dataSourceConfig.addDataSourceProperty("useServerPrepStmts", env.getProperty("spring.datasources.informix.origin.properties.useServerPrepStmts", Boolean.class));

        dataSourceConfig.validate();

        return new HikariDataSource(dataSourceConfig);
    }
}
