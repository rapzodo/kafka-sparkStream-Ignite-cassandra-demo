package com.gridu.persistence;

import org.apache.spark.api.java.JavaRDD;

import java.util.List;

public interface PersistenceStrategy<T> {
    long TTL = 60;

    void setup();

    void persist(JavaRDD<T> t);

    List<T> getAllRecords();

    void cleanUp();

}
