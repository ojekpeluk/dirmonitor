package com.firman.dirmon.aggregate;

import com.firman.dirmon.aggregate.springbatch.DomainAggregateConfig;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * This config class provides {@link FlatFileHeaderCallback} bean that will override the one in
 * {@link DomainAggregateConfig#headerCallback} to facilitate testing.
 * The latter contains timestamp in header that makes it difficult to compare expected and actual output files.
 * This one contains no timestamp in the header.
 *
 * @author
 */
@Configuration
public class DomainAggregateTestConfig {

    @Bean
    @Primary
    public FlatFileHeaderCallback headerCallback() {
        return writer -> writer.write("#");
    }

}
