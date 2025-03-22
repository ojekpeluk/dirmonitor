package com.firman.dirmon.ingest.springbatch;

import com.firman.dirmon.ingest.IngestFile;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.logging.Logger;

/**
 * @author Firman
 */
@Service
public class IngestFileService implements IngestFile {
    private static final Logger LOGGER = Logger.getLogger(IngestFileService.class.getName());

    private final JobLauncher jobLauncher;
    private final Job ingestJob;

    public IngestFileService(JobLauncher jobLauncher,
                             @Qualifier(DomainIngestConfig.JOB_NAME) Job ingestJob) {
        this.jobLauncher = jobLauncher;
        this.ingestJob = ingestJob;
    }

    /**
     * Triggers the batch job to ingest inputFile into database.
     *
     * @param filePath The file to read and insert into database.
     * @throws Exception
     */
    @Override
    public void ingestFile(String filePath) throws Exception {
        LOGGER.info("Ingesting: " + filePath);
        System.setProperty("batch.input", filePath);
        jobLauncher.run(
                ingestJob,
                new JobParametersBuilder()
                        .addLong("ingest-job-id", System.nanoTime())
                        .toJobParameters()
        );
    }
}
