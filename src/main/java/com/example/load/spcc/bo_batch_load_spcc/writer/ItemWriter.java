package com.example.load.spcc.bo_batch_load_spcc.writer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.Chunk;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@Component("writer")
@Slf4j
@StepScope
public class ItemWriter implements org.springframework.batch.item.ItemWriter<List<Map<String, Object>>>  {


    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Value("spring.datasources.target.tabla")
    private String tabla;

    public ItemWriter(@Qualifier("jdbcTemplateInsert") NamedParameterJdbcTemplate jdbcTemplate,
                         @Value("#{jobParameters['dateProcess']}") LocalDate fechaproceso,
                         @Value("#{jobParameters['acquirer']}") String acquirer ) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void write(Chunk<? extends List<Map<String, Object>>> chunk) throws Exception {



    }
}
