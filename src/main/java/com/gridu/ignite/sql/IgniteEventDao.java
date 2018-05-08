package com.gridu.ignite.sql;

import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.ignite.IgniteSparkSession;
import scala.Tuple2;

import java.util.UUID;

import static org.apache.spark.sql.functions.col;

public class IgniteEventDao implements IgniteDao<Long,Event> {
    public static final String CONFIG_FILE = "config/example-ignite.xml";
    public static final String EVENT_TABLE = "EVENT";
    private static final String EVENTS_CACHE_NAME = "events";
    private JavaIgniteContext ic;
    private CacheConfiguration<Long, Event> eventsCache;

    public IgniteEventDao(JavaSparkContext sc) {
        ic = new JavaIgniteContext(sc, IgniteConfiguration::new);
        eventsCache = new CacheConfiguration<>(EVENTS_CACHE_NAME);
        eventsCache.setIndexedTypes(Long.class,Event.class);
    }

    @Override
    public void persist(Dataset<Event> datasets){
        IgniteDao.save(datasets, EVENT_TABLE, CONFIG_FILE,"ip,url", SaveMode.Append);
    }

    @Override
    public JavaIgniteRDD<Long, Event> createAnSaveIgniteRdd(JavaRDD<Event> rdd){
        JavaIgniteRDD<Long, Event> igniteRDD = ic.<Long,Event>fromCache(eventsCache);
        igniteRDD.savePairs(rdd.mapToPair(event -> new Tuple2<>(UUID.randomUUID().getLeastSignificantBits(),event)));
        return igniteRDD;
    }


    @Override
    public Dataset<Event> getEventsDataSetFromJavaRdd(JavaIgniteRDD<Long,Event> rdd) {
        return rdd.sql("select * from Event").as(Encoders.bean(Event.class));
    }

    @Override
    public Dataset<Row> aggregateAndCountUrlActionsByIp(JavaIgniteRDD<Long,Event> rdd){
        return getEventsDataSetFromJavaRdd(rdd)
                .groupBy(col("ip"),col("url"))
                .count()
                .orderBy(col("count").desc());
    }

    @Override
    public Dataset<BotRegistry> identifyBots(Dataset<Row> aggregatedDs, long threshold) {
        return aggregatedDs
                .filter(col("count").gt(threshold))
                .as(Encoders.bean(BotRegistry.class));
    }

    @Override
    public void closeResource() {
        ic.close(true);
    }

}
