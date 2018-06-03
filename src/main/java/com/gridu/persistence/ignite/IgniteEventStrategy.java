package com.gridu.persistence.ignite;

import com.gridu.business.BotsIdentifyRules;
import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.ignite.spark.IgniteDataFrameSettings.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

public class IgniteEventStrategy implements IgniteStrategy<Long,Event> {
    private static final Logger logger = LoggerFactory.getLogger(IgniteEventStrategy.class);

    public static final String EVENT_TABLE = "EVENT";
    public static final String EVENTS_CACHE_NAME = "eventsCache";
    private JavaIgniteContext<Long,Event> ic;
    private CacheConfiguration<Long, Event> eventsCacheCfg;
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

    public Dataset<Row> fetchIpEventsCount(Dataset<Row> baseEventsDs) {
        logger.info(">>> GROUPING EVENTS BY IP <<<<");
        return baseEventsDs
                .groupBy("ip")
                .count()
                .withColumnRenamed("count","events")
                .orderBy(col("events").desc());
    }

    public Dataset<Row> fetchCategoriesByIpCount(Dataset<Row> baseEventsDs){
        logger.info(">>> GROUPING CATEGORIES BY IP <<<<");
        return baseEventsDs
                .groupBy(col("ip"),col("category"))
                .count()
                .groupBy("ip")
                .count()
                .withColumnRenamed("count","categories")
                .orderBy(col("categories").desc());
    }

    public Dataset<Row> fetchViewsAndClicksDifferenceByIp(Dataset<Row> baseEventsDs){
        logger.info(">>> GROUPING VIEWS/CLICKS DIFFERENCE BY IP <<<<");
        final Dataset<Row> viewsByIp = baseEventsDs.filter(col("type").equalTo("view"))
                .groupBy("ip")
                .count().withColumnRenamed("count","views");

        final Dataset<Row> clicksByIp = baseEventsDs.filter(col("type").equalTo("click"))
                .groupBy(col("ip"))
                .count().withColumnRenamed("count","clicks");

        return viewsByIp
                .join(clicksByIp, "ip")
                .select(col("ip"),when(col("views").isNull(),0)
                        .otherwise(col("views")).as("views"),col("clicks"))
                .select(col("ip"), col("views"), col("clicks"),
                        col("views").divide(col("clicks")).as("viewsClicksDiff"))
                .orderBy(col("viewsClicksDiff").desc());
    }

    public Dataset<Row> shortListEventsForBotsVerification(Dataset<Row> baseDS){
        logger.info(">>> PREPARING BOTS CANDIDATES SHORTLIST BY IP <<<<");
        return fetchIpEventsCount(baseDS).join(fetchViewsAndClicksDifferenceByIp(baseDS), "ip")
                .join(fetchCategoriesByIpCount(baseDS), "ip")
                .orderBy(col("viewsClicksDiff").desc());
    }

    public Dataset<BotRegistry> identifyBots(Dataset<Row> botsShortlist) {
        return botsShortlist.as(Encoders.bean(BotRegistry.class))
                .filter(new BotsIdentifyRules())
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
        return ic.ic()
                .sqlContext()
                .read()
                .format(FORMAT_IGNITE())
                .option(OPTION_TABLE(),EVENT_TABLE)
                .option(OPTION_CONFIG_FILE(),CONFIG_FILE)
                .load()
                .as(Encoders.bean(Event.class));
    }

    @Override
    public Dataset<Row> loadFromCache() {
        return ic.fromCache(eventsCacheCfg).sql("select * from " + EVENT_TABLE);
    }

    @Override
    public CacheConfiguration<Long, Event> getBotRegistryCacheConfiguration() {
        return new CacheConfiguration<Long,Event>(eventsCacheCfg);
    }

    @Override
    public void cleanUp() {
        eventsCache.clear();
    }

}
