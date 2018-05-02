package com.gridu.spark;

import com.google.common.collect.ImmutableMap;
import com.gridu.spark.processors.KafkaSinkEventStreamProcessor;
import com.gridu.spark.sql.EventDao;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Milliseconds;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class StopBotJob {

    public static final long POLL_MS = 3000;//60000
    public static final long WINDOW_MS = 600000;//60000
    public static final long SESSION_TIMEOUT_MS = 70000;
    public static final long BATCH_SIZE = 2000;
    public static final long HEARTBEAT_MS = 20000;

    public static void main(String[] args) {
        List<String> topics = Arrays.asList("events-topic");
        Map<String, Object> kafkaprops = ImmutableMap.<String, Object>builder()
                .put("bootstrap.servers", "localhost:9092")
                .put("key.deserializer", StringDeserializer.class)
                .put("value.deserializer", StringDeserializer.class)
                .put("group.id", "bot-buster-consumers")
                .put("offsets.autocommit.enable", false)
                .put("auto.offset.reset", "earliest")
                .put("consumer.session.timeout.ms", SESSION_TIMEOUT_MS)
                .put("consumer.max.poll.records", BATCH_SIZE)
                .put("consumer.group.max.session.timeout.ms", SESSION_TIMEOUT_MS)
                .put("consumer.heartbeat.interval.ms", HEARTBEAT_MS)
                .build();

        JavaStreamingContext javaStreamingContext = new JavaStreamingContext("local[*]", "stopbot",
                Milliseconds.apply(POLL_MS));

        EventDao dao = new EventDao(javaStreamingContext.sparkContext().sc());
        KafkaSinkEventStreamProcessor processor = new KafkaSinkEventStreamProcessor(topics, kafkaprops, javaStreamingContext, dao);

        processor.process();
    }
}
