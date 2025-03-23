package com.firman.dirmon;

import com.firman.dirmon.monitor.DirectoryMonitor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;

/**
 * Starts the batch process of reading input csv and writing it to database.
 *
 * @author Firman
 */
@SpringBootApplication
public class DomainBatchApplication {

    @Autowired
    private DirectoryMonitor dirMonitor;

    public static void main(String[] args) {
        SpringApplication.run(DomainBatchApplication.class, args);
    }

    @PostConstruct
    public void postConstruct() {
        dirMonitor.processEvents();
    }
}
