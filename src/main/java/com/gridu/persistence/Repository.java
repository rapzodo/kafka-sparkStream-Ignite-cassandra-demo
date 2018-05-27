package com.gridu.persistence;

import org.apache.spark.api.java.JavaRDD;

import java.util.List;

public interface Repository<T> {
    long TTL = 3;

    void setup();

    void persist(JavaRDD<T> t);

    List<T> getAllRecords();

    void cleanUp();

}
