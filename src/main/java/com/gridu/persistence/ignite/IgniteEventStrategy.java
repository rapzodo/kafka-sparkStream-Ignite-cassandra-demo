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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;

public class IgniteEventStrategy implements IgniteStrategy<Long,Event> {
    private static final Logger logger = LoggerFactory.getLogger(IgniteEventStrategy.class);

    public static final String EVENT_TABLE = "EVENT";
    public static final String EVENTS_CACHE_NAME = "eventsCache";
    private JavaIgniteContext<Long,Event> ic;
    private CacheConfiguration<Long, Event> eventsCacheCfg;
    public static final int IP__ROW_COL = 0;
    public static final int URL__ROW_COL = 1;
    public static final int COUNT_ROW_COL = 2;
    private static final long ACTIONS_THRESHOLD = 10;
    private IgniteCache<Long,Event> eventsCache;

    public IgniteEventStrategy(JavaIgniteContext ic) {
        this.ic = ic;
        setup();
    }

    @Override
    public void setup(){
        eventsCacheCfg = new CacheConfiguration<>(EVENTS_CACHE_NAME);
        eventsCacheCfg.setIndexedTypes(Long.class,Event.class);
        eventsCache = ic.ignite().getOrCreateCache(eventsCacheCfg);
    }

    public Dataset<Row> aggregateAndCountEvents() {
        final JavaIgniteRDD<Long, Event> igniteRDD = ic.fromCache(eventsCacheCfg);
        logger.info(">>> AGGREGATING EVENTS <<<<");
        return selectAggregateAndCount(igniteRDD,EVENT_TABLE,
                col("ip"),col("url"));
    }

    public Dataset<BotRegistry> identifyBots(Dataset<Row> aggregatedDs) {
        return aggregatedDs
                .filter(row -> row.getLong(COUNT_ROW_COL) > ACTIONS_THRESHOLD)
                .as(Encoders.bean(BotRegistry.class));
    }

    @Override
    public void persist(JavaRDD<Event> eventJavaRDD){
        logger.info(">>>> PERSISTING RDD IN IGNITE <<<<");
        saveIgniteRdd(eventJavaRDD, ic, eventsCacheCfg);
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
    public CacheConfiguration<Long, Event> getCacheConfiguration() {
        return new CacheConfiguration<Long,Event>(eventsCacheCfg);
    }

    @Override
    public void cleanUp() {
        eventsCache.clear();
    }

}
