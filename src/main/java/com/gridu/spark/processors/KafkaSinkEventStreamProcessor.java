package com.gridu.spark.processors;

import com.gridu.StopBotJob;
import com.gridu.converters.JsonEventMessageConverter;
import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import com.gridu.persistence.PersistenceStrategy;
import com.gridu.persistence.ignite.IgniteEventStrategy;
import com.gridu.utils.StopBotUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.Milliseconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class KafkaSinkEventStreamProcessor {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private List<String> topics;
    private Map<String, Object> props;
    private JavaStreamingContext jsc;
    private IgniteEventStrategy igniteEventStrategy;
    private PersistenceStrategy<BotRegistry> botRegistryStrategy;

    public KafkaSinkEventStreamProcessor(List<String> topics, Map<String, Object> props,
                                         JavaStreamingContext jsc,
                                         IgniteEventStrategy igniteEventStrategy,
                                         PersistenceStrategy botRegistryStrategy) {
        this.topics = topics;
        this.props = props;
        this.jsc = jsc;
        this.igniteEventStrategy = igniteEventStrategy;
        this.botRegistryStrategy = botRegistryStrategy;
    }

    public void process() {
        jsc.sparkContext().setLogLevel("INFO");

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jsc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, props));


        JavaDStream<String> eventMessages = stream.map(consumerRecord -> consumerRecord.value())
                .window(Milliseconds.apply(StopBotJob.WINDOW_MS));

        eventMessages.foreachRDD((rdd, time) -> {
            rdd.cache();
            if (rdd.count() > 0) {

                final String formattedDate = SimpleDateFormat.getDateTimeInstance().format(new Date(time.milliseconds()));
                logger.info("--------Window: {} -----------", formattedDate);

                JavaRDD<Event> eventsRDD = rdd.map(JsonEventMessageConverter::fromJson);

                igniteEventStrategy.persist(eventsRDD);

                final Dataset<Row> aggregatedEvents = igniteEventStrategy.fetchIpEventsCount().cache();
                aggregatedEvents.show(false);

                final JavaRDD<BotRegistry> identifiedBots = igniteEventStrategy.identifyBots(aggregatedEvents)
                        .toJavaRDD().cache();

                final long botsCount = identifiedBots.count();

                if (botsCount > 0) {
                    botRegistryStrategy.persist(identifiedBots);
                    igniteEventStrategy.cleanUp();
                }
            }

        });

        stream.foreachRDD(consumerRecordJavaRDD -> StopBotUtils.commitOffSets(consumerRecordJavaRDD, stream));

        jsc.start();

        try {
            jsc.awaitTermination();

        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }
}
