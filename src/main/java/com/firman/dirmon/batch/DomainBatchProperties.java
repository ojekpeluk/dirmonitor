package com.firman.dirmon.batch;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("domain")
public class DomainBatchProperties {
    private String filename;
    private int chunkSize;

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }
}
