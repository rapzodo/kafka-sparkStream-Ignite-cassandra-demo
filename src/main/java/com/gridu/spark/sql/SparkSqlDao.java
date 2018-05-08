package com.gridu.spark.sql;

import com.gridu.model.BotRegistry;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;

public interface SparkSqlDao<T> {

    Dataset<T> getEventsDataSetFromJavaRdd(JavaRDD<T> rdd);

    Dataset<BotRegistry> findBots(JavaRDD<T> rdd, long threshold);

    void closeResource();
}
