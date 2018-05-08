package com.gridu.ignite.sql;

import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;

public interface IgniteDao<K,T> {

    JavaIgniteRDD<K,T> createAnSaveIgniteRdd(JavaRDD<T> rdd);

    Dataset<T> getEventsDataSetFromJavaRdd(JavaIgniteRDD<K,T> rdd);

    Dataset<BotRegistry> findBots(Dataset<T> aggregatedDs, long threshold);

    void closeResource();
}
