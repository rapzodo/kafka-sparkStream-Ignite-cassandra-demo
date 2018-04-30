package com.gridu.spark.sql;

import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class EventDao {

    private SparkSession session;

    public void setLoggerLevel(String level){
        session.sparkContext().setLogLevel(level);
    }

    public EventDao(SparkSession session){
        this.session = session;
    }

    public EventDao(SparkContext sparkContext){
        session = SparkSession.builder().sparkContext(sparkContext).getOrCreate();
    }

    public EventDao(String master, String appName){
        session = SparkSession.builder().master(master).appName(appName).getOrCreate();
    }

    public EventDao(SparkConf sparkConf){
        session = SparkSession.builder().config(sparkConf).getOrCreate();
    }

    public Dataset<BotRegistry> aggregateAndCountIpUrlActions(RDD<Event> rdd) {
        Dataset<Event> eventDS = session.createDataset(rdd, Encoders.bean(Event.class));
        Dataset<BotRegistry> result = eventDS.select(col("ip"),col("type"), col("url"))
                .groupBy(col("ip"),col("type"), col("url"))
                .count().orderBy(col("count").desc())
                .map(row -> new BotRegistry(row.getString(0),row.getString(2),row.getLong(3))
                        , Encoders.bean(BotRegistry.class));
        result.show();
        return result;
    }

}
