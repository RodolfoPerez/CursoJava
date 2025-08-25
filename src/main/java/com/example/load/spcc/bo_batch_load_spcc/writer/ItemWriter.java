package com.example.load.spcc.bo_batch_load_spcc.writer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.Chunk;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.*;

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

        try {

        List<MapSqlParameterSource> batchValues = new ArrayList<>();

        String sql = String.format("INSERT INTO %s (id_adquirente,cuenta,referencia,monto) VALUES (:adquirente,:cuenta,:referencia,:monto)",tabla);


        for(Map<String, Object> item : chunk.getItems().get(0)) {

            MapSqlParameterSource parameterSource = new MapSqlParameterSource();

            parameterSource.addValue("adquirente",item.get("bancoAdquirente"));
            parameterSource.addValue("cuenta",item.get("cuenta"));
            parameterSource.addValue("referencia","000000");
            parameterSource.addValue("cuenta", Objects.equals(item.get("monto").toString(), "1000") ? "1250" : item.get("monto"));

            batchValues.add(parameterSource);

        }

        int[] resultado = this.jdbcTemplate.batchUpdate(sql,batchValues.toArray(new MapSqlParameterSource[0]));

        int totalInsert = Arrays.stream(resultado).sum();

        log.info("Total Insertados hasta ahora {} en la tabla {}",totalInsert,tabla);

        } catch (RuntimeException e) {
            log.error("Error al insertar: {}", e.getMessage());
            System.exit(-1);
        }
    }
}
