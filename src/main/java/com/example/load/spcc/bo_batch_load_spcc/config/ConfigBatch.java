package com.example.load.spcc.bo_batch_load_spcc.config;

import com.example.load.spcc.bo_batch_load_spcc.writer.ItemWriterCommerce;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;

import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;
import java.util.Map;
import org.springframework.batch.core.job.flow.Flow;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Configuration
public class ConfigBatch {

    private final JdbcTemplate jdbcTemplate;

    @Value("${spring.params.chunk-size}")
    private Integer chunkSize;

    @Value("${spring.datasources.informix.origin.tabla-origen}")
    private String tabla;


    @Value("${spring.params.fetch-size}")
    private Integer fetchSize;


    public static int getPageIndex() {
        return pageIndex.get();
    }

    public static int getPageIndexAndIncrement() {
        return pageIndex.getAndIncrement();
    }

    public static void setPageIndex(int value) {
        pageIndex.set(value);
    }

    public static int getMaxPages() {
        return maxPages.get();
    }

    public static void setMaxPages(int value) {
        maxPages.set(value);
    }

    private static AtomicInteger pageIndex = new AtomicInteger(0);
    private static AtomicInteger maxPages = new AtomicInteger(0);



    public ConfigBatch(@Qualifier("jdbcTemplateInfx") JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }


    @Bean
    public Job finalExchangeStep1(JobRepository jobRepository,
                                  @Qualifier("deleteRecords") Step deleteRecords) {
        return new JobBuilder("Step 1", jobRepository)
                .start(deleteRecords)
                .build();
    }


    @Bean
    public Job finalExchangeStep2(JobRepository jobRepository,
                                  @Qualifier("extractionRecords") Step extractionRecords) {

        Flow[] flows = this.getFlows(extractionRecords);

        Flow extractionRecordsFlow = new FlowBuilder<Flow>("flowsExtraccionStep2")
                .split(new SimpleAsyncTaskExecutor())
                .add(flows)
                .build();

        return new JobBuilder("Step 2", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(extractionRecordsFlow)
                .end()
                .listener(listenerJob)
                .build();
    }

    @Bean
    public Job finalExchangeStepALL(JobRepository jobRepository,
                                    @Qualifier("deleteRecords") Step deleteRecords,
                                    @Qualifier("extractionRecords") Step extractionRecords) {

        Flow[] flows = this.getFlows(extractionRecords);

        Flow extractionRecordsFlow = new FlowBuilder<Flow>("flowsExtraccionALL")
                .split(new SimpleAsyncTaskExecutor())
                .add(flows)
                .build();

        Flow flowDelete = new FlowBuilder<Flow>("deleteAllPreviosRegisters")
                .from(deleteRecords)
                .end();

        return new JobBuilder("Step all", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(flowDelete)
                .next(extractionRecordsFlow)
                .end()
                .listener(listenerJob)
                .build();
    }

    @Bean(name = "deleteRecords")
    public Step deleteRecords(JobRepository jobRepository,
                              @Qualifier("reprocessTasklet") Tasklet deleteRecordsTasklet,
                              PlatformTransactionManager transactionManager) {

        return new StepBuilder("Delete records", jobRepository)
                .tasklet(deleteRecordsTasklet, transactionManager)
                .build();
    }


    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(64);
        executor.setMaxPoolSize(64);
        executor.setQueueCapacity(64);
        executor.setThreadNamePrefix("MultiThread- ");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        return executor;
    }


    private Flow[] getFlows(Step step) {


        String sql = String.format("select count(*) from %s", tabla);
        Long totalRows = this.jdbcTemplate.queryForObject(sql, Long.class);
        Long totalPages = totalRows / this.fetchSize + ((totalRows % this.fetchSize > 0) ? 1 : 0);
        maxPages.set(totalPages.intValue());
        log.info("TotalRows {}, TotalPages {}", String.format("%,d", totalRows), totalPages);

        Long totalParts = totalPages / 4 + ((totalRows % 4 > 0) ? 1 : 0);

        log.info("Total parts {}", totalParts);

        Flow[] flows = new Flow[totalParts.intValue()];

        for (int i = 0; i < totalParts; i++) {

            flows[i] = new FlowBuilder<Flow>("Extractionpart " + i)
                    .from(step)
                    .end();
        }

        return flows;

    }

    private JobExecutionListener listenerJob = new JobExecutionListener() {

        @Override
        public void beforeJob(JobExecution jobExecution) {
            ItemWriterCommerce.setTotalRecordsInserted(0);
        }

        @Override
        public void afterJob(JobExecution jobExecution) {
            log.info("Registros insertados en total : {}", String.format("%,d", ItemWriterCommerce.getTotalRecordsInserted()));
        }
    };

    @Bean(name = "extractionRecords")
    public Step extractionsRecords(JobRepository jobRepository,
                                   @Qualifier("reader") ItemReader<List<Map<String, Object>>> reader,
                                   @Qualifier("writer") ItemWriter<List<Map<String, Object>>> writer,
                                   PlatformTransactionManager transactionManager) {

        return new StepBuilder("extraction records", jobRepository)
                .<List<Map<String, Object>>, List<Map<String, Object>>>chunk(this.chunkSize, transactionManager)
                .taskExecutor(taskExecutor())
                .reader(reader)
                .writer(writer)
                .build();

    }

}
