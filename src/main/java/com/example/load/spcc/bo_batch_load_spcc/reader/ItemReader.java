package com.example.load.spcc.bo_batch_load_spcc.reader;


import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@Component("reader")
@Slf4j
@StepScope
public class ItemReader implements org.springframework.batch.item.ItemReader<List<Map<String, Object>>> {

    private final JdbcTemplate jdbcTemplate;

    @Value("spring.datasources.informix.origin.tabla-origen")
    private String tabla;

    private LocalDate fechaproceso;
    private String acquirer;

    public ItemReader(@Qualifier("jdbcTemplateInfx") JdbcTemplate jdbcTemplate,
                         @Value("#{jobParameters['dateProcess']}") LocalDate fechaproceso,
                         @Value("#{jobParameters['acquirer']}") String acquirer ) {
        this.jdbcTemplate = jdbcTemplate;
        this.fechaproceso = fechaproceso;
        this.acquirer= acquirer;
    }

    @Override
    public List<Map<String, Object>> read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {

        try{

        String sql = String.format("select * from %s where fe_proceso = '%s' and adquirente = %s",tabla,fechaproceso,acquirer);
        log.info("Query : " + sql);

            return this.jdbcTemplate.queryForList(sql);

        } catch (RuntimeException e) {
            log.error("Error al ejecutar el query de select en la tabla {} con adquirente {}",tabla,acquirer);
            System.exit(-1);
        }

        return null;
    }
}
