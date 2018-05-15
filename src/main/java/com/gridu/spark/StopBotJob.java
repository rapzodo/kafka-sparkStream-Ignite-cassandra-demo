package com.gridu.spark;

import com.google.common.collect.ImmutableMap;
import com.gridu.business.BotRegistryBusinessService;
import com.gridu.business.EventsBusinessService;
import com.gridu.model.BotRegistry;
import com.gridu.persistence.BaseDao;
import com.gridu.persistence.cassandra.CassandraDao;
import com.gridu.persistence.ignite.IgniteEventDao;
import com.gridu.spark.processors.KafkaSinkEventStreamProcessor;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.Milliseconds;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class StopBotJob {

    public static final long POLL_MS = 60000;//60000
    public static final long WINDOW_MS = 600000;//60000
    public static final long SESSION_TIMEOUT_MS = 70000;
    public static final long BATCH_SIZE = 2000;
    public static final long HEARTBEAT_MS = 20000;

    public static void main(String[] args) {
        Logger.getLogger("org.apache.ignite").setLevel(Level.INFO);
        try(Ignite ignite = Ignition.start()) {
            Ignition.setClientMode(true);
            List<String> topics = Arrays.asList("partners-events-topic");
            Map<String, Object> kafkaProps = getConfiguration();

            JavaStreamingContext javaStreamingContext = new JavaStreamingContext("local[*]", "stopbot",
                    Milliseconds.apply(POLL_MS));

            final JavaIgniteContext<?, ?> igniteContext = new JavaIgniteContext<>(javaStreamingContext.sparkContext()
                    , IgniteConfiguration::new);

//            final IgniteBotRegistryDao botRegistryDao = new IgniteBotRegistryDao(igniteContext);

            final BaseDao<BotRegistry> botRegistryDao = new CassandraDao(javaStreamingContext.sparkContext().sc());

            BotRegistryBusinessService botRegistryBusinessService = new BotRegistryBusinessService(botRegistryDao);

            final IgniteEventDao eventDao = new IgniteEventDao(igniteContext);
            EventsBusinessService eventsBusinessService = new EventsBusinessService(eventDao);

            KafkaSinkEventStreamProcessor processor = new KafkaSinkEventStreamProcessor(topics, kafkaProps, javaStreamingContext,
                    eventsBusinessService, botRegistryBusinessService);

            processor.process();
        }
    }

    private static ImmutableMap<String, Object> getConfiguration() {
        return ImmutableMap.<String, Object>builder()
                .put("bootstrap.servers", "localhost:9092")
                .put("key.deserializer", StringDeserializer.class)
                .put("value.deserializer", StringDeserializer.class)
                .put("group.id", "bot-buster-consumers")
                .put("offsets.autocommit.enable", false)
                .put("consumer.auto.offset.reset", "latest")
                .put("consumer.session.timeout.ms", SESSION_TIMEOUT_MS)
                .put("consumer.max.poll.records", BATCH_SIZE)
                .put("consumer.group.max.session.timeout.ms", SESSION_TIMEOUT_MS)
                .put("consumer.heartbeat.interval.ms", HEARTBEAT_MS)
                .build();
    }
}
