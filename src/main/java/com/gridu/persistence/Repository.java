package com.gridu.persistence;

import org.apache.spark.sql.Dataset;

import java.util.List;

public interface Repository<T> {
    long TTL = 3;

    void setup();

    void persist(Dataset<T> t);

    List<T> getAllRecords();

    void cleanUp();

}
