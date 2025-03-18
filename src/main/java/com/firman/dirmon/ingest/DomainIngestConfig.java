package com.firman.dirmon.ingest;

import com.firman.dirmon.datasource.Domain;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.logging.Logger;

/**
 * Spring Batch configuration for input file ingestion into the database.
 *
 * @author Firman
 */
@Configuration
public class DomainIngestConfig {

    private static final Logger LOGGER = Logger.getLogger(DomainIngestConfig.class.getName());
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
    private static final String READER_NAME = "domainReader";
    private static final String STEP_NAME = "ingestDomainStep";
    private static final String JOB_NAME = "ingestDomainJob";

    private final String inputFile;
    private final int chunkSize;
    private final DataSource dataSource;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final JobLauncher jobLauncher;

    public DomainIngestConfig(@Value("${batch.input}") String inputFile,
                              @Value("${batch.chunk-size}") int chunkSize,
                              DataSource dataSource,
                              JobRepository jobRepository,
                              PlatformTransactionManager transactionManager,
                              JobLauncher jobLauncher) {
        this.inputFile = inputFile;
        this.chunkSize = chunkSize;
        this.dataSource = dataSource;
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.jobLauncher = jobLauncher;
    }

    @Bean
    public FlatFileItemReader<Domain> csvReader() {
        LOGGER.info("Ingesting: " + inputFile);
        return new FlatFileItemReaderBuilder<Domain>()
                .name(READER_NAME)
                .resource(new FileSystemResource(inputFile))
                .linesToSkip(1)
                .delimited()
                .names(INPUT_FIELDS)
                .targetType(Domain.class)
                .build();
    }


    @Bean
    public JdbcBatchItemWriter<Domain> dbWriter() {
        return new JdbcBatchItemWriterBuilder<Domain>()
                .sql(INSERT_SQL)
                .dataSource(dataSource)
                .beanMapped()
                .build();
    }


    @Bean
    public Step ingestStep() {
        return new StepBuilder(STEP_NAME)
                .repository(jobRepository)
                .<Domain, Domain>chunk(chunkSize)
                .reader(csvReader())
                .writer(dbWriter())
                .transactionManager(transactionManager)
                .build();
    }

    @Bean
    public Job ingestJob() {
        return new JobBuilder(JOB_NAME)
                .repository(jobRepository)
                .start(ingestStep())
                .build();
    }

    public void runJob() throws Exception {
        jobLauncher.run(
                ingestJob(),
                new JobParametersBuilder()
                        .addLong("ingest-job-id", System.nanoTime())
                        .toJobParameters()
        );
    }
}
