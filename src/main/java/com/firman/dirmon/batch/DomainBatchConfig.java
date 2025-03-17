package com.firman.dirmon.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.logging.Logger;

/**
 * Main job configuration file.
 * The batch job will be executed on demand each time file system monitor detects new file created in watch dir.
 *
 * @author Firman
 */
@Configuration
public class DomainBatchConfig {

    private static final Logger LOGGER = Logger.getLogger(DomainBatchConfig.class.getName());
    private static final String INSERT_SQL = "INSERT INTO domains "
            + "(timestamp, src_ip, src_port, dst_ip, dst_port, domain)"
            + " VALUES "
            + "(:timestamp, :srcIp, :srcPort, :dstIp, :dstPort, :domain)";
    private static final String INPUT_FIELDS[] = {"timestamp",
            "srcIp",
            "srcPort",
            "dstIp",
            "dstPort",
            "domain"};
    private static final String READER_NAME = "domainsReader";
    private static final String STEP_NAME = "readWriteDomainsStep";
    private static final String JOB_NAME = "ingestDomainsJob";

    @Bean
    public FlatFileItemReader<Domain> domainReader(DomainBatchProperties properties) {
        LOGGER.info("Ingesting: " + properties.getFilename());
        return new FlatFileItemReaderBuilder<Domain>()
                .name(READER_NAME)
                .resource(new FileSystemResource(properties.getFilename()))
                .linesToSkip(1)
                .delimited()
                .names(INPUT_FIELDS)
                .targetType(Domain.class)
                .build();
    }


    @Bean
    public JdbcBatchItemWriter<Domain> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Domain>()
                .sql(INSERT_SQL)
                .dataSource(dataSource)
                .beanMapped()
                .build();
    }


    @Bean
    public Step step(JobRepository jobRepository,
                     PlatformTransactionManager transactionManager,
                     ItemReader<Domain> reader,
                     ItemWriter<Domain> writer,
                     DomainBatchProperties properties) {
        return new StepBuilder(STEP_NAME)
                .repository(jobRepository)
                .<Domain, Domain>chunk(properties.getChunkSize())
                .reader(reader)
                .writer(writer)
                .transactionManager(transactionManager)
                .build();
    }

    @Bean
    public Job job(JobRepository jobRepository,
                   Step step) {
        return new JobBuilder(JOB_NAME)
                .repository(jobRepository)
                .start(step)
                .build();
    }
}
