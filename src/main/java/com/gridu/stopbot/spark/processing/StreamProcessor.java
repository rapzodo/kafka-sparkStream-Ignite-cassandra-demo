package com.gridu.stopbot.spark.processing;

import com.google.common.collect.ImmutableMap;
import com.gridu.stopbot.spark.processing.utils.OffsetUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class StreamProcessor implements EventsProcessor {

    private List<String> topics;

    private Map<String, Object> props;

//    For testing purpose
    {
        topics = Arrays.asList("partners-events-topic");
        props = ImmutableMap.<String, Object>builder()
            .put("bootstrap.servers", "localhost:9092")
            .put("key.deserializer", StringDeserializer.class)
            .put("value.deserializer", StringDeserializer.class)
            .put("group.id", "bot-buster-consumers")
            .put("offsets.autocommit.enable", false)
            .build();
    }

    public StreamProcessor(List<String> topics, Map<String, Object> props) {
        this.topics = topics;
        this.props = props;
    }

    public void process(boolean offsetsAutoCommit) {
        JavaStreamingContext jsc = new JavaStreamingContext("local[*]", "appName2", Duration.apply(3));
        jsc.sparkContext().setLogLevel("ERROR");

        JavaInputDStream<ConsumerRecord<String, String>> events = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, props));

        if (!offsetsAutoCommit){
            events.foreachRDD(rdd -> OffsetUtils.commitOffSets(rdd, events));
        }
        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
