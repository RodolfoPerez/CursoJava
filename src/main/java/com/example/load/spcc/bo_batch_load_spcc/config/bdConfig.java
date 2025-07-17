package com.example.load.spcc.bo_batch_load_spcc.config;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.HashMap;

@Configuration
@Slf4j
public class bdConfig {

    @Autowired
    private Environment env;

    String username = "";
    String password = "";



    @Bean(name = "target")
    public DataSource targetconfDataSource() {


            log.info("Creando conexion de BD");
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

        dataSourceConfig.setJdbcUrl(env.getProperty("target.sql.datasource-target.jdbc-url"));
        dataSourceConfig.setUsername(username);
        dataSourceConfig.setPassword(password);
        dataSourceConfig.setDriverClassName(env.getProperty("target.sql.datasource-target.driver-class-name"));
        dataSourceConfig.setPoolName(env.getProperty("target.sql.datasource-target.poolName"));
        dataSourceConfig.setMaximumPoolSize(env.getProperty("target.sql.datasource-target.maximumPoolSize", Integer.class));
        dataSourceConfig.setMinimumIdle(env.getProperty("target.sql.datasource-target.minimumIdle", Integer.class));
        dataSourceConfig.setConnectionTimeout(300000);
        dataSourceConfig.setIdleTimeout(180000);
        dataSourceConfig.addDataSourceProperty("cachePrepStmts", env.getProperty("target.sql.datasource-target.properties.cachePrepStmts", Boolean.class));
        dataSourceConfig.addDataSourceProperty("prepStmtCacheSize", env.getProperty("target.sql.datasource-target.properties.prepStmtCacheSize", Integer.class));
        dataSourceConfig.addDataSourceProperty("prepStmtCacheSqlLimit", env.getProperty("target.sql.datasource-target.properties.prepStmtCacheSqlLimit", Integer.class));
        dataSourceConfig.addDataSourceProperty("useServerPrepStmts", env.getProperty("target.sql.datasource-target.properties.useServerPrepStmts", Boolean.class));

        dataSourceConfig.validate();

        return new HikariDataSource(dataSourceConfig);
    }

}
