package com.firman.dirmon.datasource;

/**
 * Represents output csv
 */
public class OutputCsv {
    private int rowNumber;
    private String domain;
    private String connections;

    public int getRowNumber() {
        return rowNumber;
    }

    public void setRowNumber(int rowNumber) {
        this.rowNumber = rowNumber;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getConnections() {
        return connections;
    }

    public void setConnections(String connections) {
        this.connections = connections;
    }
}
