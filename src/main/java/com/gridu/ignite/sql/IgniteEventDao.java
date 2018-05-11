package com.gridu.ignite.sql;

import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;

public class IgniteEventDao implements IgniteDao<Long,Event> {
    public static final String CONFIG_FILE = "config/example-ignite.xml";
    public static final String EVENT_TABLE = "EVENT";
    public static final String EVENTS_CACHE_NAME = "events";
    private JavaIgniteContext<Long,Event> ic;
    private CacheConfiguration<Long, Event> eventsCacheCfg;

    public IgniteEventDao(JavaSparkContext sc) {
        ic = new JavaIgniteContext(sc, IgniteConfiguration::new);
        eventsCacheCfg = new CacheConfiguration<>(EVENTS_CACHE_NAME);
        eventsCacheCfg.setIndexedTypes(Long.class,Event.class);
//        eventsCacheCfg.setSqlSchema("PUBLIC");
    }

    @Override
    public void persist(Dataset<Event> datasets){
        IgniteDao.save(datasets, EVENT_TABLE, CONFIG_FILE,"ip,datetime,url","template=replicated",SaveMode.Append);
    }

    @Override
    public JavaIgniteRDD<Long, Event> createAnSaveIgniteRdd(JavaRDD<Event> rdd){
        JavaIgniteRDD<Long, Event> igniteRDD = ic.<Long,Event>fromCache(eventsCacheCfg);
        igniteRDD.clear();
        igniteRDD.savePairs(rdd.mapToPair(event -> new Tuple2<>(IgniteDao.generateIgniteUuid(),event)));
        return igniteRDD;
    }




    @Override
    public Dataset<Event> getDataSetFromIgniteJavaRdd(JavaIgniteRDD<Long,Event> rdd) {
        return rdd.sql("select * from " + EVENT_TABLE).as(Encoders.bean(Event.class));
    }

    @Override
    public Dataset<Row> aggregateAndCount(Dataset<Event> eventDataset, Column... groupedCols){
        return eventDataset
                .groupBy(groupedCols)
//                .groupBy(col("ip"),col("url"))
                .count()
                .orderBy(col("count").desc());
    }

    @Override
    public List<Event> getAllRecords() {
        CacheConfiguration<Long,Event> configuration = new CacheConfiguration<Long, Event>(EVENTS_CACHE_NAME)
                .setSqlSchema("PUBLIC");
        IgniteCache<Long, Event> blacklistCache = ic.ignite().getOrCreateCache(configuration);

        List<List<?>> all = blacklistCache
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

        return null;
    }

    @Override
    public void closeResource() {
        ic.close(true);
    }

}
