package com.firman.dirmon.ingest;

/**
 * @author Firman
 */
public interface IngestFile {
    /**
     * Ingest the given file to backing datasource.
     *
     * @param filePath
     */
    void ingestFile(String filePath) throws Exception;
}
