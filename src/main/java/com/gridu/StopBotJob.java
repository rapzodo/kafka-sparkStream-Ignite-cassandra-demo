package com.gridu;

import com.google.common.collect.ImmutableMap;
import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import com.gridu.persistence.PersistenceStrategy;
import com.gridu.persistence.cassandra.CassandraStrategy;
import com.gridu.persistence.ignite.IgniteEventStrategy;
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

    public static final long POLL_MS = 60000;//1min
    public static final long WINDOW_MS = 600000;//10min
    public static final long SESSION_TIMEOUT_MS = 70000;
    public static final long BATCH_SIZE = 2000;
    public static final long HEARTBEAT_MS = 20000;
    public static final ImmutableMap<String, Object> KAFKA_PROPS = ImmutableMap.<String, Object>builder()
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

    public static final List<String> TOPICS = Arrays.asList("partners-events-topic");

    public static void main(String[] args) {
        setLogLevels();
        try(Ignite ignite = Ignition.start()) {
            Map<String, Object> kafkaProps = KAFKA_PROPS;

            JavaStreamingContext javaStreamingContext = new JavaStreamingContext("local[*]", "stopbot",
                    Milliseconds.apply(POLL_MS));

            final JavaIgniteContext<Long, Event> igniteContext = new JavaIgniteContext<>(javaStreamingContext.sparkContext()
                    , IgniteConfiguration::new);

//            final IgniteBotRegistryStrategy botRegistryDao = new IgniteBotRegistryStrategy(igniteContext);

            final IgniteEventStrategy igniteEventStrategy = new IgniteEventStrategy(igniteContext);

            final PersistenceStrategy<BotRegistry> cassandraService = new CassandraStrategy(javaStreamingContext.sparkContext().sc());

            KafkaSinkEventStreamProcessor processor = new KafkaSinkEventStreamProcessor(TOPICS, kafkaProps, javaStreamingContext,
                    igniteEventStrategy, cassandraService);

            processor.process();
        }
    }

    private static void setLogLevels(){
        Logger.getLogger("com.gridu").setLevel(Level.INFO);
        Logger.getLogger("org.apache.ignite").setLevel(Level.ERROR);
    }
}
