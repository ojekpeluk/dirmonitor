package com.firman.dirmon;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Starts the batch process of reading input csv and writing it to database.
 *
 * @author Firman
 */
@EnableBatchProcessing
@SpringBootApplication
public class DomainBatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(DomainBatchApplication.class, args);
    }

}
