package com.firman.dirmon.aggregate;

import com.firman.dirmon.datasource.Domain;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.logging.Logger;

/**
 * Spring Batch config for scheduled batch to read and aggregate database data.
 */
@Configuration
@EnableScheduling
public class DomainAggregateConfig {

    private static final Logger LOGGER = Logger.getLogger(DomainAggregateConfig.class.getName());
    private static final String SELECT_SQL = "SELECT timestamp, src_ip, src_port, dst_ip, dst_port, domain FROM domains";
    private static final String[] OUTPUT_FIELDS = {
            "timestamp",
            "srcIp",
            "srcPort",
            "dstIp",
            "dstPort",
            "domain"
    };
    private static final String WRITER_NAME = "domainWriter";
    private static final String STEP_NAME = "aggregateDomainStep";
    private static final String JOB_NAME = "aggregareDomainJob";

    private final String outputFile;
    private final int chunkSize;
    private final DataSource dataSource;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final JobLauncher jobLauncher;

    public DomainAggregateConfig(@Value("${batch.output}") String outputFile,
                                 @Value("${batch.chunk-size}") int chunkSize,
                                 DataSource dataSource,
                                 JobRepository jobRepository,
                                 PlatformTransactionManager transactionManager,
                                 JobLauncher jobLauncher) {
        this.outputFile = outputFile;
        this.chunkSize = chunkSize;
        this.dataSource = dataSource;
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.jobLauncher = jobLauncher;
    }

    @Bean
    public RowMapper<Domain> domainRowMapper() {
        return (resultSet, rowNum) -> {
            Domain domain = new Domain();
            domain.setTimestamp(resultSet.getLong("timestamp"));
            domain.setSrcIp(resultSet.getString("src_ip"));
            domain.setSrcPort(resultSet.getInt("src_port"));
            domain.setDstIp(resultSet.getString("dst_ip"));
            domain.setDstPort(resultSet.getInt("dst_port"));
            domain.setDomain(resultSet.getString("domain"));
            return domain;
        };
    }

    @Bean
    public JdbcCursorItemReader<Domain> dbReader() {
        return new JdbcCursorItemReaderBuilder<Domain>()
                .name(WRITER_NAME)
                .dataSource(dataSource)
                .sql(SELECT_SQL)
                .rowMapper(domainRowMapper())
                .build();
    }

    @Bean
    public DelimitedLineAggregator<Domain> domainDelimitedLineAggregator() {
        BeanWrapperFieldExtractor<Domain> beanWrapperFieldExtractor = new BeanWrapperFieldExtractor<>();
        beanWrapperFieldExtractor.setNames(OUTPUT_FIELDS);

        DelimitedLineAggregator<Domain> domainDelimitedLineAggregator = new DelimitedLineAggregator<>();
        domainDelimitedLineAggregator.setDelimiter(",");
        domainDelimitedLineAggregator.setFieldExtractor(beanWrapperFieldExtractor);
        return domainDelimitedLineAggregator;
    }

    @Bean
    public FlatFileItemWriter<Domain> csvWriter() {
        return new FlatFileItemWriterBuilder<Domain>()
                .name(WRITER_NAME)
                .resource(new FileSystemResource(outputFile))
                .lineAggregator(domainDelimitedLineAggregator())
                .build();
    }

    @Bean
    public Step domainAggregateStep() {
        return new StepBuilder(STEP_NAME)
                .repository(jobRepository)
                .<Domain, Domain>chunk(chunkSize)
                .reader(dbReader())
                .writer(csvWriter())
                .transactionManager(transactionManager)
                .build();
    }

    @Bean
    public Job domainAggregateJob() {
        LOGGER.info("Creating domain aggregator job");
        return new JobBuilder(JOB_NAME)
                .repository(jobRepository)
                .start(domainAggregateStep())
                .build();
    }

    /**
     * Schedule this batch job to run once every minute (fixedRate = 60000).
     *
     * @throws Exception
     */
    @Scheduled(fixedRate = 60000)
    public void runJob() throws Exception {
        jobLauncher.run(
                domainAggregateJob(),
                new JobParametersBuilder()
                        .addLong("aggregate-job-id", System.nanoTime())
                        .toJobParameters()
        );
    }
}
