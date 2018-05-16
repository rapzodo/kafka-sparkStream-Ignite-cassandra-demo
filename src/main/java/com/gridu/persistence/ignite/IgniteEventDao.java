package com.gridu.persistence.ignite;

import com.gridu.model.Event;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class IgniteEventDao implements IgniteDao<Long,Event> {
    public static final String EVENT_TABLE = "EVENT";
    public static final String EVENTS_CACHE_NAME = "events";
    private JavaIgniteContext<Long,Event> ic;
    private CacheConfiguration<Long, Event> eventsCacheCfg;
    private IgniteCache<Long,Event> eventsCache;

    public IgniteEventDao(JavaIgniteContext ic) {
        this.ic = ic;
        setup();
    }

    @Override
    public void setup(){
        eventsCacheCfg = new CacheConfiguration<>(EVENTS_CACHE_NAME);
        eventsCacheCfg.setIndexedTypes(Long.class,Event.class);
        eventsCache = ic.ignite().getOrCreateCache(eventsCacheCfg);
    }

    @Override
    public void persist(Dataset<Event> datasets){
        IgniteDao.save(datasets, EVENT_TABLE, CONFIG_FILE,"ip,datetime,url","template=replicated",SaveMode.Ignore);
    }

    @Override
    public JavaIgniteRDD<Long, Event> createAnSaveIgniteRdd(JavaRDD<Event> rdd){
        JavaIgniteRDD<Long, Event> igniteRDD = ic.<Long,Event>fromCache(eventsCacheCfg);
        igniteRDD.savePairs(rdd.mapToPair(event -> new Tuple2<>(IgniteDao.generateIgniteUuid(),event)));
        return igniteRDD;
    }

    @Override
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
