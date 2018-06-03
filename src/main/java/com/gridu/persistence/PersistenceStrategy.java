package com.gridu.persistence;

import com.gridu.utils.StopBotUtils;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;

public interface PersistenceStrategy<T> {
    long TTL = Long.valueOf(StopBotUtils.getProperty("bots.ttl.days","1"));

    void setup();

    void persist(JavaRDD<T> t);

    List<T> getAllRecords();

    void cleanUp();

}
