package com.firman.dirmon.aggregate;

import com.firman.dirmon.aggregate.springbatch.DomainAggregateConfig;
import com.firman.dirmon.ingest.springbatch.DomainIngestConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.batch.test.AssertFile.assertFileEquals;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {DomainIngestConfig.class, DomainAggregateConfig.class, DomainAggregateTestConfig.class})
@TestPropertySource(locations = "classpath:application.properties")
@EnableAutoConfiguration
public class DomainAggregateConfigTest {

    @Autowired
    @Qualifier(DomainAggregateConfig.JOB_NAME)
    private Job aggregateJob;
    @Autowired
    @Qualifier(DomainIngestConfig.JOB_NAME)
    private Job ingestJob;
    @Autowired
    private JobLauncher jobLauncher;

    @Test
    public void testAggregateJob() throws Exception {
        File input = new File("src/test/resources/input3.csv");
        FileSystemResource expectedResult = new FileSystemResource("src/test/resources/expected-aggregate-output.csv");
        jobLauncher.run(
                ingestJob,
                new JobParametersBuilder()
                        .addLong("id", System.nanoTime())
                        .addString("batch.input", input.getAbsolutePath())
                        .toJobParameters()
        );
        JobExecution execution = jobLauncher.run(
                aggregateJob,
                new JobParametersBuilder()
                        .addLong("id", System.nanoTime())
                        .toJobParameters()
        );
        assertNotNull(execution);
        assertEquals(BatchStatus.COMPLETED, execution.getStatus());
        FileSystemResource actualResult = new FileSystemResource("output.csv");
        assertFileEquals(expectedResult, actualResult);
    }


}
