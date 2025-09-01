package com.example.load.spcc.bo_batch_load_spcc.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
public class ConfigBatch {

    @Value("${spring.params.chunk-size}")
    private Integer chunkSize;


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
        return new JobBuilder("Step 2", jobRepository)
                .start(extractionRecords)
                .build();
    }

    @Bean
    public Job finalExchangeStepALL(JobRepository jobRepository,
                                    @Qualifier("deleteRecords") Step deleteRecords,
                                    @Qualifier("extractionRecords") Step extractionRecords) {
        return new JobBuilder("Step all", jobRepository)
                .start(deleteRecords)
                .next(extractionRecords)
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
    public TaskExecutor taskExecutor(){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(64);
        executor.setMaxPoolSize(64);
        executor.setQueueCapacity(64);
        executor.setThreadNamePrefix("MultiThread- ");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        return executor;
    }


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
