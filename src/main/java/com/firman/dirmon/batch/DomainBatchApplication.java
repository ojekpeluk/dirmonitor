package com.firman.dirmon.batch;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Starts the batch process of reading input csv and writing it to database.
 *
 * @author Firman
 */
@EnableBatchProcessing
@EnableConfigurationProperties(DomainBatchProperties.class)
@SpringBootApplication
public class DomainBatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(DomainBatchApplication.class, args);
    }

}
