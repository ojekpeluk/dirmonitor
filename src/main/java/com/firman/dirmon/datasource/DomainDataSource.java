package com.firman.dirmon.datasource;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.logging.Logger;

/**
 * Ingest and aggregate batch functions will need this to talk to database.
 *
 * @author Firman
 */
@Configuration
public class DomainDataSource {
    private static final Logger LOGGER = Logger.getLogger(DomainDataSource.class.getName());

    @Bean
    @Primary
    @ConfigurationProperties("datasource")
    public HikariDataSource dataSource() {
        LOGGER.info("Creating datasource");
        return DataSourceBuilder.create().type(HikariDataSource.class).build();
    }
}
