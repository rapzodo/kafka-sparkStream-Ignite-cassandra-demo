package com.gridu.persistence.ignite;

import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;

public class IgniteEventService implements IgniteService<Long,Event> {
    private static final Logger logger = LoggerFactory.getLogger(IgniteEventService.class);

    public static final String EVENT_TABLE = "EVENT";
    public static final String EVENTS_CACHE_NAME = "events";
    private JavaIgniteContext<Long,Event> ic;
    private CacheConfiguration<Long, Event> eventsCacheCfg;
    private IgniteCache<Long,Event> eventsCache;
    public static final int IP__ROW_COL = 0;
    public static final int URL__ROW_COL = 1;
    public static final int COUNT_ROW_COL = 2;
    private static final long ACTIONS_THRESHOLD = 10;

    public IgniteEventService(JavaIgniteContext ic) {
        this.ic = ic;
        setup();
    }

    @Override
    public void setup(){
        eventsCacheCfg = new CacheConfiguration<>(EVENTS_CACHE_NAME);
        eventsCacheCfg.setIndexedTypes(Long.class,Event.class);
        eventsCache = ic.ignite().getOrCreateCache(eventsCacheCfg);
    }

    public Dataset<Row> saveAggregateAndCountEvents(JavaRDD<Event> eventJavaRDD) {
        logger.info(">>> AGGREGATING DATASET <<<<");
        return selectAggregateAndCount(createAnSaveIgniteRdd(eventJavaRDD),EVENT_TABLE,
                col("ip"),col("url"));
    }

    public JavaIgniteRDD<Long, Event> createAnSaveIgniteRdd(JavaRDD<Event> rdd){
        logger.info(">>>> PERSISTING RDD IN IGNITE <<<<");
        JavaIgniteRDD<Long, Event> igniteRDD = ic.<Long,Event>fromCache(eventsCacheCfg);
        igniteRDD.savePairs(rdd.mapToPair(event -> new Tuple2<>(IgniteService.generateIgniteUuid(),event)));
        return igniteRDD;
    }

    public Dataset<BotRegistry> identifyBots(Dataset<Row> aggregatedDs) {
        return aggregatedDs
                .filter(row -> row.getLong(COUNT_ROW_COL) > ACTIONS_THRESHOLD)
                .as(Encoders.bean(BotRegistry.class));
    }

    @Override
    public void persist(Dataset<Event> datasets){
        createAnSaveIgniteRdd(datasets.toJavaRDD());
    }

    public Dataset<Event> getDataSetFromIgniteJavaRdd(JavaIgniteRDD<Long,Event> rdd) {
        return rdd.sql("select * from " + EVENT_TABLE).as(Encoders.bean(Event.class));
    }

    @Override
    public List<Event> getAllRecords() {
        List<List<?>> all = eventsCache
                .query(new SqlFieldsQuery("select * from " + EVENT_TABLE))
                .getAll();

        return all.stream().map(objects -> new Event(objects.get(0).toString(),
                objects.get(1).toString(),
                Long.valueOf(objects.get(2).toString()),
                objects.get(3).toString()))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public Dataset<Event> loadFromIgnite() {
        return ic.ic().sqlContext()
                .read()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .load()
                .as(Encoders.bean(Event.class));
    }

    @Override
    public void cleanUp() {
        eventsCache.destroy();
        ic.close(true);
    }

}
