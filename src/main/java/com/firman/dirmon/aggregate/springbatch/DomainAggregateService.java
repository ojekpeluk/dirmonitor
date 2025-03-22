package com.firman.dirmon.aggregate.springbatch;

import com.firman.dirmon.aggregate.DomainAggregate;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Service to periodically triggers spring batch job to agrregate data.
 *
 * @author Firman
 */
@Service
@EnableScheduling
public class DomainAggregateService implements DomainAggregate {

    private final JobLauncher jobLauncher;
    private final Job aggregateJob;

    public DomainAggregateService(JobLauncher jobLauncher,
                                  @Qualifier(DomainAggregateConfig.JOB_NAME) Job aggregateJob) {
        this.jobLauncher = jobLauncher;
        this.aggregateJob = aggregateJob;
    }

    /**
     * Schedule the batch job to run every minute (fixedRate = 60000)
     *
     * @throws Exception
     */
    @Override
    @Scheduled(fixedRate = 60000)
    public void aggregate() throws Exception {
        jobLauncher.run(
                aggregateJob,
                new JobParametersBuilder()
                        .addLong("aggregate-job-id", System.nanoTime())
                        .toJobParameters()
        );
    }
}
