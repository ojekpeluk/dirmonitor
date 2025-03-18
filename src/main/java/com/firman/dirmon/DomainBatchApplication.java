package com.firman.dirmon;

import com.firman.dirmon.ingest.DirMonitor;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;

/**
 * Starts the batch process of reading input csv and writing it to database.
 *
 * @author Firman
 */
@EnableBatchProcessing
@SpringBootApplication
public class DomainBatchApplication {

    @Autowired
    private DirMonitor dirMonitor;

    public static void main(String[] args) {
        SpringApplication.run(DomainBatchApplication.class, args);
    }

    @PostConstruct
    public void postConstruct() {
        dirMonitor.processEvents();
    }
}
