package com.firman.dirmon.aggregate;

import com.firman.dirmon.datasource.OutputCsv;
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
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.logging.Logger;

/**
 * Spring Batch config for scheduled batch to read and aggregate database data.
 */
@Configuration
@EnableScheduling
public class DomainAggregateConfig {

    private static final Logger LOGGER = Logger.getLogger(DomainAggregateConfig.class.getName());
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final String SELECT_SQL = "SELECT ROW_NUMBER() OVER () as row, domain, connections FROM (SELECT domain, COUNT(*) as connections from domains GROUP BY domain ORDER BY connections DESC LIMIT 10)";
    private static final String[] OUTPUT_FIELDS = {"rowNumber", "domain", "connections"};
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
    public RowMapper<OutputCsv> domainRowMapper() {
        return (resultSet, rowNum) -> {
            OutputCsv outputCsv = new OutputCsv();
            outputCsv.setRowNumber(resultSet.getInt("row"));
            outputCsv.setDomain(resultSet.getString("domain"));
            outputCsv.setConnections(resultSet.getLong("connections") + " connections");
            return outputCsv;
        };
    }

    @Bean
    public JdbcCursorItemReader<OutputCsv> dbReader() {
        return new JdbcCursorItemReaderBuilder<OutputCsv>()
                .name(WRITER_NAME)
                .dataSource(dataSource)
                .sql(SELECT_SQL)
                .rowMapper(domainRowMapper())
                .build();
    }

    @Bean
    public DelimitedLineAggregator<OutputCsv> domainDelimitedLineAggregator() {
        BeanWrapperFieldExtractor<OutputCsv> beanWrapperFieldExtractor = new BeanWrapperFieldExtractor<>();
        beanWrapperFieldExtractor.setNames(OUTPUT_FIELDS);

        DelimitedLineAggregator<OutputCsv> domainDelimitedLineAggregator = new DelimitedLineAggregator<>();
        domainDelimitedLineAggregator.setDelimiter(" - ");
        domainDelimitedLineAggregator.setFieldExtractor(beanWrapperFieldExtractor);
        return domainDelimitedLineAggregator;
    }

    @Bean
    public FlatFileItemWriter<OutputCsv> csvWriter() {
        return new FlatFileItemWriterBuilder<OutputCsv>()
                .name(WRITER_NAME)
                .resource(new FileSystemResource(outputFile))
                .lineAggregator(domainDelimitedLineAggregator())
                .headerCallback(writer -> writer.write("# Top 10 domains " + DATE_FORMAT.format(Calendar.getInstance().getTime())))
                .build();
    }

    @Bean
    public Step domainAggregateStep() {
        return new StepBuilder(STEP_NAME)
                .repository(jobRepository)
                .<OutputCsv, OutputCsv>chunk(chunkSize)
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
