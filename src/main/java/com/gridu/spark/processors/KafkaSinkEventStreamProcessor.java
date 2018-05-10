package com.gridu.spark.processors;

import com.gridu.business.BotRegistryBusinessService;
import com.gridu.business.EventsBusinessService;
import com.gridu.converters.JsonEventMessageConverter;
import com.gridu.ignite.sql.IgniteBotRegistryDao;
import com.gridu.ignite.sql.IgniteEventDao;
import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import com.gridu.spark.StopBotJob;
import com.gridu.spark.utils.OffsetUtils;
import org.apache.ignite.spark.JavaIgniteRDD;
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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class KafkaSinkEventStreamProcessor implements EventsProcessor {

    private List<String> topics;
    private Map<String, Object> props;
    private JavaStreamingContext jsc;
    private BotRegistryBusinessService botRegistryBusinessService;
    private EventsBusinessService eventsBusinessService;

    public KafkaSinkEventStreamProcessor(List<String> topics, Map<String, Object> props,
                                         JavaStreamingContext jsc,
                                         BotRegistryBusinessService botRegistryBusinessService,
                                         EventsBusinessService eventsBusinessService) {
        this.topics = topics;
        this.props = props;
        this.jsc = jsc;
        this.botRegistryBusinessService = botRegistryBusinessService;
        this.eventsBusinessService = eventsBusinessService;
    }

    public void process() {
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jsc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, props));


        JavaDStream<String> eventMessages = stream.map(consumerRecord -> consumerRecord.value())
                .window(Milliseconds.apply(StopBotJob.WINDOW_MS));

        eventMessages.foreachRDD((rdd, time) -> {
            rdd.cache();
            if (rdd.count() > 0) {
                System.out.println("--------Window: " + SimpleDateFormat.getDateTimeInstance().format(new Date(time.milliseconds())) + "-------");
                JavaRDD<Event> eventsRDD = rdd.map(JsonEventMessageConverter::fromJson);

                long start = System.currentTimeMillis();

                final Dataset<BotRegistry> bots = eventsBusinessService.execute(eventsRDD);
                if(bots.count() > 0) botRegistryBusinessService.execute(bots);

                System.out.println("Exec time >>>>>> " + (System.currentTimeMillis() - start));

                //TODO persist bots to blacklist

            }

        });
        stream.foreachRDD(consumerRecordJavaRDD -> OffsetUtils.commitOffSets(consumerRecordJavaRDD, stream));

        jsc.start();

        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            jsc.sparkContext().getConf().log().error(e.getMessage(), e);
        }
    }
}
