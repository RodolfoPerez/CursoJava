package com.example.load.spcc.bo_batch_load_spcc.tasklets;


import com.github.lalyos.jfiglet.FigletFont;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

@Component("reprocessTasklet")
@Slf4j
@StepScope
public class DeleteTasklet implements Tasklet {

    private final JdbcTemplate jdbcTemplate;

    @Value("${spring.datasources.target.tabla}")
    private String tabla;

    private LocalDate fechaproceso;
    private String acquirer;

    public DeleteTasklet(@Qualifier("jdbcTemplate") JdbcTemplate jdbcTemplate,
                         @Value("#{jobParameters['dateProcess']}") LocalDate fechaproceso,
                         @Value("#{jobParameters['acquirer']}") String acquirer) {
        this.jdbcTemplate = jdbcTemplate;
        this.fechaproceso = fechaproceso;
        this.acquirer = acquirer;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        log.info("\n" + FigletFont.convertOneLine("Step 1 Reproceso"));
        try {
            String sql = String.format("delete %s where fe_alta_eglobal = ? and id_adquirente = %s ", tabla, acquirer);
            int totalCount = this.jdbcTemplate.update(sql, fechaproceso);
            log.info("Total de registros eliminados {} de la tabla {} con adquirente {}", totalCount, tabla, acquirer);

        } catch (RuntimeException e) {
            log.error("Error al ejecutar el query de eliminacion en la tabla {} con adquirente {} {}", tabla, acquirer, e);
            System.exit(-1);
        }
        return RepeatStatus.FINISHED;
    }
}
