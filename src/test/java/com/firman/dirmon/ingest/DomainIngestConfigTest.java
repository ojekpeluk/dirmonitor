package com.firman.dirmon.ingest;

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
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = DomainIngestConfig.class)
@TestPropertySource(locations = "classpath:application.properties")
@EnableAutoConfiguration
public class DomainIngestConfigTest {

    @Autowired
    @Qualifier(DomainIngestConfig.JOB_NAME)
    private Job ingestJob;
    @Autowired
    private JobLauncher jobLauncher;

    @Test
    public void testIngestJob() throws Exception {
        File input1 = new File("src/test/resources/input1.csv");
        JobExecution execution = jobLauncher.run(
                ingestJob,
                new JobParametersBuilder()
                        .addLong("id", System.nanoTime())
                        .addString("batch.input", input1.getAbsolutePath())
                        .toJobParameters()
        );
        assertNotNull(execution);
        assertEquals(BatchStatus.COMPLETED, execution.getStatus());
    }

    @Test
    public void testIngestJob_duplicated_input() throws Exception {
        // The following contains duplicated input
        File input2 = new File("src/test/resources/input2.csv");
        JobExecution execution = jobLauncher.run(
                ingestJob,
                new JobParametersBuilder()
                        .addLong("id", System.nanoTime())
                        .addString("batch.input", input2.getAbsolutePath())
                        .toJobParameters()
        );
        assertNotNull(execution);
        assertEquals(BatchStatus.FAILED, execution.getStatus());
    }

}
