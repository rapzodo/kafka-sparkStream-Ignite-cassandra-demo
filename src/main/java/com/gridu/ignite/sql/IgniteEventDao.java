package com.gridu.ignite.sql;

import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.ignite.IgniteSparkSession;
import scala.Tuple2;

import java.util.UUID;

import static org.apache.spark.sql.functions.col;

public class IgniteEventDao implements IgniteDao<Long,Event> {
    public static final String EVENTS_CACHE_NAME = "Events";
    public static final String CONFIG_FILE = "config/example-ignite.xml";
    private JavaIgniteContext ic;
    private CacheConfiguration<Long, Event> eventsCache;

    public IgniteEventDao(JavaIgniteContext ic) {
        this.ic = ic;
    }

    private void showDataTables(){
        IgniteSparkSession igniteSession = IgniteSparkSession.builder()
                .appName("Spark Ignite catalog example")
                .master("local")
                .config("spark.executor.instances", "2")
                //Only additional option to refer to Ignite cluster.
                .igniteConfig(CONFIG_FILE)
                .getOrCreate();

// This will print out info about all SQL tables existed in Ignite.
        igniteSession.catalog().listTables().show();
    }

    public void saveDataset(JavaRDD<Event> rdd){
        Dataset<Event> eventDataset = ic.ic().sqlContext().createDataset(rdd.rdd(), Encoders.bean(Event.class));
        eventDataset.write()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_TABLE(),"Event")
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG_FILE)
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(),"ip,datetime,url")
                .mode(SaveMode.Overwrite)
                .save();
    }

    public JavaIgniteRDD<Long, Event> createAnSaveIgniteRdd(JavaRDD<Event> rdd){
        CacheConfiguration<Long, Event> cacheConfiguration = new CacheConfiguration<>(EVENTS_CACHE_NAME);
        cacheConfiguration.setIndexedTypes(Long.class,Event.class);

        JavaIgniteRDD<Long, Event> igniteRDD = ic.<Long,Event>fromCache(cacheConfiguration);
        igniteRDD.savePairs(rdd.mapToPair(event -> new Tuple2<>(UUID.randomUUID().getLeastSignificantBits(),event)));
        return igniteRDD;
    }


    @Override
    public Dataset<Event> getEventsDataSetFromJavaRdd(JavaIgniteRDD<Long,Event> rdd) {
        return rdd.sql("select * from Event").as(Encoders.bean(Event.class));
    }

    public Dataset<BotRegistry> aggregateAndCountUrlActionsByIp(JavaIgniteRDD<Long,Event> rdd){
        return getEventsDataSetFromJavaRdd(rdd)
                .groupBy(col("ip"),col("url"),col("type"))
                .count()
                .as(Encoders.bean(BotRegistry.class));
    }

    @Override
    public Dataset<BotRegistry> findBots(Dataset<Event> aggregatedDs, long threshold) {
        return aggregatedDs
                .filter(col("count").gt(threshold))
                .as(Encoders.bean(BotRegistry.class));
    }

    @Override
    public void closeResource() {
        ic.close(true);
    }

}
