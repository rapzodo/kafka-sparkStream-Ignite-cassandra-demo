package com.gridu;

import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;

public interface BaseDao {

    Dataset<Event> getEventDataSet(JavaRDD<Event> rdd);

    Dataset<BotRegistry> findBots(JavaRDD<Event> rdd, long threshold);
}
