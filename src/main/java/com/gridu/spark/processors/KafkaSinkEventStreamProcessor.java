package com.gridu.spark.processors;

import com.gridu.StopBotJob;
import com.gridu.converters.JsonEventMessageConverter;
import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import com.gridu.persistence.Repository;
import com.gridu.persistence.ignite.IgniteEventService;
import com.gridu.utils.OffsetUtils;
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
    private IgniteEventService eventsBusinessService;
    private Repository<BotRegistry> botRegistryRepository;

    public KafkaSinkEventStreamProcessor(List<String> topics, Map<String, Object> props,
                                         JavaStreamingContext jsc,
                                         IgniteEventService eventsBusinessService,
                                         Repository botRegistryRepository) {
        this.topics = topics;
        this.props = props;
        this.jsc = jsc;
        this.eventsBusinessService = eventsBusinessService;
        this.botRegistryRepository = botRegistryRepository;
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

                final String formattedString = SimpleDateFormat.getDateTimeInstance().format(new Date(time.milliseconds()));
                logger.info("--------Window: {} -----------", formattedString);

                JavaRDD<Event> eventsRDD = rdd.map(JsonEventMessageConverter::fromJson);

                final Dataset<Row> bots = eventsBusinessService.saveAggregateAndCountEvents(eventsBusinessService.createAnSaveIgniteRdd(eventsRDD));

                final Dataset<BotRegistry> botRegistryDataset = eventsBusinessService.identifyBots(bots).cache();

                final long botsCount = botRegistryDataset.count();

                if (botsCount > 0) {
                    botRegistryRepository.persist(botRegistryDataset);
                }
            }

        });

        stream.foreachRDD(consumerRecordJavaRDD -> OffsetUtils.commitOffSets(consumerRecordJavaRDD, stream));

        jsc.start();

        try {
            jsc.awaitTermination();

        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }
}
