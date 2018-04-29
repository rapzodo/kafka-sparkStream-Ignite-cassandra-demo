package com.gridu.stopbot.spark.processors;

import com.google.common.collect.ImmutableMap;
import com.gridu.stopbot.spark.processors.utils.OffsetUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.util.Map;

public class BatchProcessor implements EventsProcessor {

    private Map<String, Object> params = ImmutableMap.<String, Object>builder()
            .put("bootstrap.servers","localhost:9092")
            .put("key.deserializer", StringDeserializer.class)
            .put("value.deserializer", StringDeserializer.class)
            .put("group.id", "bot-buster-consumers")
            .put("offsets.autocommit.enable",false)
            .build();

    private String topic = "partners-events-topic";

    public void process(boolean offsetsAutoCommit) {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "appName2");
        sc.setLogLevel("ERROR");

        OffsetRange[] offsetRanges = OffsetUtils.createOffsetRanges(topic, 0, 1, 100, 1);

        KafkaUtils.createRDD(sc,params,offsetRanges,LocationStrategies.PreferConsistent());

//        events.foreachRDD( rdd -> {
//            OffsetUtils.commitOffSets(rdd, events);
//        });

    }
}
