package com.example.load.spcc.bo_batch_load_spcc.reader;


import com.example.load.spcc.bo_batch_load_spcc.config.ConfigBatch;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component("reader")
@Slf4j
@StepScope
public class ItemReaderCommerce implements org.springframework.batch.item.ItemReader<List<Map<String, Object>>> {

    private final JdbcTemplate jdbcTemplate;

    @Value("${spring.datasources.informix.origin.tabla-origen}")
    private String tabla;

    @Value("${spring.params.fetch-size}")
    private Integer fetchSize;

    private String acquirer;


    public ItemReaderCommerce(@Qualifier("jdbcTemplateInfx") JdbcTemplate jdbcTemplate,
                              @Value("#{jobParameters['acquirer']}") String acquirer) {
        this.jdbcTemplate = jdbcTemplate;
        this.acquirer = acquirer;
    }

    @Override
    public List<Map<String, Object>> read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {

        try {

            int pageTmp = ConfigBatch.getPageIndexAndIncrement();
            if (pageTmp > ConfigBatch.getMaxPages())
                return null;


            int skip = fetchSize * pageTmp;

            String sql = String.format("select id_sucursal,no_afiliacion,razon_social,calle_no,colonia " +
                    "from %s where id_adquirente = '%s' " +
                    " order by rowid SKIP %s LIMIT %s ", tabla, acquirer, skip, fetchSize);

            if (skip == 0) {
                log.info("Query inicial : " + sql);
            }


            List<Map<String, Object>> listFinded = this.jdbcTemplate.queryForList(sql);

            if (listFinded == null || listFinded.isEmpty())
                return null;

            return listFinded;

        } catch (RuntimeException e) {
            log.error("Error al ejecutar el query de select en la tabla {} con adquirente {} {}", tabla, acquirer, e);
            System.exit(-1);
        }

        return null;
    }
}
