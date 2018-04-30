package com.gridu.stopbot.spark.processors;

import com.gridu.stopbot.converters.JsonConverter;
import com.gridu.stopbot.model.BotRegistry;
import com.gridu.stopbot.model.Event;
import com.gridu.stopbot.spark.processors.utils.OffsetUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
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

import static org.apache.spark.sql.functions.col;

public class KafkaSinkEventStreamProcessor implements EventsProcessor {

    private List<String> topics;
    private Map<String, Object> props;
    public static final long CLICKS_THRESHOLD = 18;
    private SparkSession session;
    private JavaStreamingContext jsc;

    public KafkaSinkEventStreamProcessor(List<String> topics, Map<String, Object> props,JavaStreamingContext jsc) {
        this.topics = topics;
        this.props = props;
        this.session=session;
        this.jsc=jsc;
    }

    public void process(boolean offsetsAutoCommit) {
        jsc.sparkContext().setLogLevel("ERROR");
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, props));

        JavaDStream<Event> events = stream.map(consumerRecord -> JsonConverter.fromJson(consumerRecord.value()))
                .window(Minutes.apply(10), Seconds.apply(3));;
        events.foreachRDD((rdd, time) -> {
            session = SparkSession.builder().sparkContext(jsc.sparkContext().sc()).getOrCreate();

            Dataset<Event> eventsDS = session.createDataset(rdd.rdd(), Encoders.bean(Event.class));

            Dataset<BotRegistry> botRegistryDS = findBots(eventsDS);
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

    public Dataset<BotRegistry> aggregateAndCountIpUrlActions(Dataset<Event> eventDataset) {
        Dataset<BotRegistry> result = eventDataset.select(col("ip"), col("url"))
                .groupBy(col("ip"), col("url"))
                .count().orderBy(col("count").desc())
                .map(row -> new BotRegistry(row.getString(0),row.getString(1),row.getLong(2))
                        , Encoders.bean(BotRegistry.class));
        result.show();
        return result;
    }

    public Dataset<BotRegistry> findBots(Dataset<Event> botRegistryDataset){
        return aggregateAndCountIpUrlActions(botRegistryDataset)
                .filter(botRegistry -> botRegistry.getNumberOfActions() > CLICKS_THRESHOLD);

    }
}
