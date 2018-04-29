package com.gridu.stopbot.spark.processors;

import com.google.common.collect.ImmutableMap;
import com.gridu.stopbot.converters.JsonConverter;
import com.gridu.stopbot.model.Event;
import com.gridu.stopbot.model.QueryResult;
import com.gridu.stopbot.spark.processors.utils.OffsetUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class KafkaSinkEventStreamProcessor implements EventsProcessor {

    private List<String> topics;

    private Map<String, Object> props;

    public static final long CLICKS_THRESHOLD = 10;

    private SparkSession session;

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

    public KafkaSinkEventStreamProcessor(List<String> topics, Map<String, Object> props, SparkSession session) {
        this.topics = topics;
        this.props = props;
        this.session=session;
    }

    public void process(boolean offsetsAutoCommit) {
        JavaStreamingContext jsc = new JavaStreamingContext("local[*]", "appName2", Duration.apply(3));
        jsc.sparkContext().setLogLevel("ERROR");

        JavaDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, props))
                .window(Minutes.apply(10), Minutes.apply(1));

        JavaDStream<Event> events = stream.map(consumerRecord -> JsonConverter.fromJson(consumerRecord.value()));
        events.foreachRDD((rdd, time) -> {
            countUrlActions(rdd);
        });

        if (!offsetsAutoCommit){
            stream.foreachRDD(rdd -> OffsetUtils.commitOffSets(rdd, stream));
        }
        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public boolean exceedClicks(QueryResult queryResult) {
        return queryResult.getNumberOfActions() > CLICKS_THRESHOLD;
    }

    public Dataset<QueryResult> countUrlActions(JavaRDD<Event> rdd) {
        Dataset<Event> events = session.createDataset(rdd.rdd(), Encoders.bean(Event.class));
        Dataset<QueryResult> result = events.select(col("ip"), col("url"))
                .groupBy(col("ip"), col("url"))
                .count().orderBy(col("count").desc())
//                .withColumnRenamed("result","actions")
                .map(row -> new QueryResult(row.getString(0),row.getString(1),row.getLong(2))
                        , Encoders.bean(QueryResult.class));
        result.show();
        return result;
    }
}
