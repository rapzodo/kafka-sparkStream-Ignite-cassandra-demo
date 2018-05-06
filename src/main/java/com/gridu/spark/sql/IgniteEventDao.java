package com.gridu.spark.sql;

import com.gridu.BaseDao;
import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spark.IgniteRDD;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;

import static org.apache.spark.sql.functions.col;

public class IgniteEventDao implements BaseDao {
    private JavaIgniteContext ic;

    public IgniteEventDao(JavaSparkContext sc) {
        ic = new JavaIgniteContext(sc, IgniteConfiguration::new, false);
    }

    public JavaIgniteRDD<Integer, Event> persistRddToCache(JavaRDD<Event> rdd){
        JavaIgniteRDD<Integer, Event> igniteRDD = ic.fromCache("events");
        igniteRDD.saveValues(rdd);
        return  igniteRDD;
    }

    @Override
    public Dataset<Event> getEventDataSet(JavaRDD<Event> rdd) {
        JavaIgniteRDD<Integer, Event> eventIgniteRDD = persistRddToCache(rdd);
        return eventIgniteRDD.sql("SELECT type, ip, url FROM events")
                .as(Encoders.bean(Event.class));
    }

    @Override
    public Dataset<BotRegistry> findBots(JavaRDD<Event> rdd, long threshold) {
        return getEventDataSet(rdd)
                .groupBy(col("ip"),col("url"),col("type"))
                .count()
                .filter(col("count").gt(threshold))
                .as(Encoders.bean(BotRegistry.class));
    }
}
