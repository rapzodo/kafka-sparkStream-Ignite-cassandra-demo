package com.gridu.persistence;

import com.gridu.utils.StopBotIgniteUtils;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;

public interface PersistenceStrategy<T> {
    long TTL = Long.valueOf(StopBotIgniteUtils.getProperty("bots.ttl.days"));

    void setup();

    void persist(JavaRDD<T> t);

    List<T> getAllRecords();

    void cleanUp();

}
