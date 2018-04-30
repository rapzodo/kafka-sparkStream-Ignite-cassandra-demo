package com.gridu.spark.processors;

import com.gridu.converters.JsonConverter;
import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import com.gridu.spark.utils.OffsetUtils;
import com.gridu.spark.sql.EventDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.List;
import java.util.Map;

public class KafkaSinkEventStreamProcessor implements EventsProcessor {

    private List<String> topics;
    private Map<String, Object> props;
    public static final long CLICKS_THRESHOLD = 18;
    private JavaStreamingContext jsc;
    private EventDao eventDao;

    public KafkaSinkEventStreamProcessor(List<String> topics, Map<String, Object> props,
                                         JavaStreamingContext jsc, EventDao eventDao) {
        this.topics = topics;
        this.props = props;
        this.jsc=jsc;
        this.eventDao = eventDao;
    }

    public void process(boolean offsetsAutoCommit) {
        jsc.sparkContext().setLogLevel("ERROR");
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, props));

        JavaDStream<Event> events = stream.map(consumerRecord -> JsonConverter.fromJson(consumerRecord.value()))
                .window(Minutes.apply(10), Seconds.apply(3));;
        events.foreachRDD((rdd, time) -> {
            Dataset<BotRegistry> botRegistryDS = identifyBots(rdd.rdd());
            botRegistryDS.show();
            //TODO persist bot to blacklist

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

    public Dataset<BotRegistry> identifyBots(RDD<Event> rdd){
        return eventDao.aggregateAndCountIpUrlActions(rdd)
                .filter(botRegistry -> botRegistry.getNumberOfActions() > CLICKS_THRESHOLD);

    }
}
