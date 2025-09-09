package com.example.load.spcc.bo_batch_load_spcc.writer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.Chunk;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Component("writer")
@Slf4j
@StepScope
public class ItemWriterCommerce implements org.springframework.batch.item.ItemWriter<List<Map<String, Object>>> {


    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Value("${spring.datasources.target.tabla}")
    private String tabla;

    private final LocalDate fechaproceso;
    private final String acquirer;


    public static int getTotalRecordsInserted() {
        return totalRecordsInserted.get();
    }

    public static void setTotalRecordsInserted(int value) {
        totalRecordsInserted.set(value);
    }

    private static AtomicInteger totalRecordsInserted = new AtomicInteger(0);

    public ItemWriterCommerce(@Qualifier("jdbcTemplateInsert") NamedParameterJdbcTemplate jdbcTemplate,
                              @Value("#{jobParameters['dateProcess']}") LocalDate fechaproceso,
                              @Value("#{jobParameters['acquirer']}") String acquirer) {
        this.jdbcTemplate = jdbcTemplate;
        this.fechaproceso = fechaproceso;
        this.acquirer = acquirer;
    }

    @Override
    public void write(Chunk<? extends List<Map<String, Object>>> chunk) throws Exception {

        try {

            List<MapSqlParameterSource> batchValues = new ArrayList<>();

            String sql = String.format("INSERT INTO %s (id_sucursal,no_afiliacion,razon_social,calle_no,colonia,fe_alta_eglobal,id_adquirente) " +
                    "VALUES (:id_sucursal,:no_afiliacion,:razon_social,:calle_no,:colonia,:fe_alta_eglobal,:id_adquirente)", tabla);


            for (Map<String, Object> item : chunk.getItems().get(0)) {

                MapSqlParameterSource parameterSource = new MapSqlParameterSource();

                parameterSource.addValue("id_sucursal", item.get("id_sucursal"));
                parameterSource.addValue("no_afiliacion", item.get("no_afiliacion"));
                parameterSource.addValue("razon_social", item.get("razon_social"));
                parameterSource.addValue("calle_no", item.get("calle_no"));
                parameterSource.addValue("colonia", item.get("colonia"));
                parameterSource.addValue("fe_alta_eglobal", fechaproceso);
                parameterSource.addValue("id_adquirente", acquirer);

                batchValues.add(parameterSource);

            }

            int[] resultado = this.jdbcTemplate.batchUpdate(sql, batchValues.toArray(new MapSqlParameterSource[0]));
            int totalInsert = Arrays.stream(resultado).sum();

            totalRecordsInserted.addAndGet(totalInsert);

            log.info("Total Insertados hasta ahora {} en la tabla {}", String.format("%,d",totalRecordsInserted.get()), tabla);

        } catch (RuntimeException e) {
            log.error("Error al insertar: {}", e.getMessage());
            System.exit(-1);
        }
    }
}
