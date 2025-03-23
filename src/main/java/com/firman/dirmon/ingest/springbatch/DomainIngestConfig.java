package com.firman.dirmon.ingest.springbatch;

import com.firman.dirmon.datasource.Domain;
import com.firman.dirmon.monitor.DirMonitorService;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.logging.Logger;

/**
 * Spring Batch configuration for input file ingestion into the database.
 * The batch job runs just once and gets triggered by {@link DirMonitorService} whenever it gets notified of new input file.
 *
 * @author Firman
 */
@Configuration
@EnableBatchProcessing
public class DomainIngestConfig {

    private static final Logger LOGGER = Logger.getLogger(DomainIngestConfig.class.getName());
    private static final String INSERT_SQL = "INSERT INTO domains "
            + "(timestamp, src_ip, src_port, dst_ip, dst_port, domain)"
            + " VALUES "
            + "(:timestamp, :srcIp, :srcPort, :dstIp, :dstPort, :domain)";
    private static final String[] INPUT_FIELDS = {"timestamp",
            "srcIp",
            "srcPort",
            "dstIp",
            "dstPort",
            "domain"};
    private static final String READER_NAME = "domainReader";
    private static final String STEP_NAME = "ingestDomainStep";
    public static final String JOB_NAME = "ingestDomainJob";

    /**
     * The input file being copied to watched directory,
     * that is then ingested to database. It is initialized to a value set in property file.
     * However for each job, before the job runs, it is set to new value by {@link DirMonitorService}.
     */
    private final int chunkSize;
    private final DataSource dataSource;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    public DomainIngestConfig(@Value("${batch.chunk-size}") int chunkSize,
                              DataSource dataSource,
                              JobRepository jobRepository,
                              PlatformTransactionManager transactionManager) {
        this.chunkSize = chunkSize;
        this.dataSource = dataSource;
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
    }

    /**
     * Read input file passed on through job params, and ingest it into db.
     *
     * @param inputFile Batch input file.
     * @return
     */
    @Bean("ingestReader")
    @StepScope
    public FlatFileItemReader<Domain> csvReader(@Value("#{jobParameters['batch.input']}") String inputFile) {
        return new FlatFileItemReaderBuilder<Domain>()
                .name(READER_NAME)
                .resource(new FileSystemResource(inputFile))
                .linesToSkip(1)
                .delimited()
                .names(INPUT_FIELDS)
                .targetType(Domain.class)
                .build();
    }

    @Bean("ingestWriter")
    public JdbcBatchItemWriter<Domain> dbWriter() {
        return new JdbcBatchItemWriterBuilder<Domain>()
                .sql(INSERT_SQL)
                .dataSource(dataSource)
                .beanMapped()
                .build();
    }

    @Bean("ingestStep")
    public Step ingestStep(@Qualifier("ingestReader") ItemReader<Domain> reader,
                           @Qualifier("ingestWriter") ItemWriter<Domain> writer) {
        return new StepBuilder(STEP_NAME)
                .repository(jobRepository)
                .<Domain, Domain>chunk(chunkSize)
                .reader(reader)
                .writer(writer)
                .transactionManager(transactionManager)
                .build();
    }

    @Bean(JOB_NAME)
    public Job ingestJob(@Qualifier("ingestStep") Step ingestStep) {
        return new JobBuilder(JOB_NAME)
                .repository(jobRepository)
                .start(ingestStep)
                .build();
    }

}
