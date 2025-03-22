package com.firman.dirmon.aggregate;

/**
 * Aggregates data from datasource and writes it out.
 *
 * @author Firman
 */
public interface DomainAggregate {

    void aggregate() throws Exception;

}
