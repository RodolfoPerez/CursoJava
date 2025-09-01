package com.example.load.spcc.bo_batch_load_spcc;

import com.example.load.spcc.bo_batch_load_spcc.utils.BatchUtils;
import com.github.lalyos.jfiglet.FigletFont;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.LocalDate;

@SpringBootApplication
@Slf4j
public class BoBatchLoadSpccApplication implements ApplicationRunner {


    private final Job finalExchangeStep1;
    private final Job finalExchangeStep2;
    private final Job finalExchangeStepALL;
    private final JobLauncher jobLauncher;

    public BoBatchLoadSpccApplication(Job finalExchangeStep1, Job finalExchangeStep2, Job finalExchangeStepALL, JobLauncher jobLauncher) {

        this.finalExchangeStep1 = finalExchangeStep1;
        this.finalExchangeStep2 = finalExchangeStep2;
        this.finalExchangeStepALL = finalExchangeStepALL;
        this.jobLauncher = jobLauncher;

    }

    public static void main(String[] args) {

        System.exit(SpringApplication.exit(SpringApplication.run(BoBatchLoadSpccApplication.class, args)));

    }

    @Override
    public void run(ApplicationArguments args) throws Exception {

        log.info("\n" + FigletFont.convertOneLine("Curso Java"));
        log.info("Total de parametros recibidos: {}", args.getSourceArgs().length - 1);


        String acquirer = BatchUtils.getArgumentValue(args, "acquirer");
        LocalDate dateProcess = BatchUtils.getArgumentValueAsLocalDate(args, "dateProcess", "yyyyMMdd");
        String step = BatchUtils.getArgumentValue(args, "step").toUpperCase();

        if (!step.equals("1") && !step.equals("2") && !step.equals("ALL")) {
            log.error("Step desconocido: {}", step);
            System.exit(1);
        }

        log.info("=====================================");
        log.info("|  Ejecutando batch con parámetros:  ");
        log.info("|  ----------------|---------------");
        log.info("|  Fecha proceso   |   {}              ", dateProcess);
        log.info("|  Adquirente      |   {}              ", acquirer);
        log.info("|  Step            |   {}              ", step);
        log.info("=====================================");


        JobParameters jobParameters = new JobParametersBuilder()
                .addLocalDate("dateProcess", dateProcess)
                .addString("acquirer", acquirer)
                .toJobParameters();

        switch (step) {
            case "1":
                this.jobLauncher.run(this.finalExchangeStep1, jobParameters);
                break;
            case "2":
                this.jobLauncher.run(this.finalExchangeStep2, jobParameters);
                break;
            case "ALL":
                this.jobLauncher.run(this.finalExchangeStepALL, jobParameters);
                break;
            default:
                log.error("Step desconocido {}", step);
                System.exit(1);
        }
    }
}

