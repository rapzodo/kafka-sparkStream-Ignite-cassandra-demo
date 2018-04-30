import com.google.common.collect.ImmutableMap;
import com.gridu.converters.JsonConverter;
import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import com.gridu.spark.processors.KafkaSinkEventStreamProcessor;
import com.gridu.spark.sql.EventDao;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaSinkEventStreamProcessorTest{
    private KafkaSinkEventStreamProcessor processor;
    private List<String> topics;
    private Map<String,Object> kafkaprops;
    private Dataset<Row> rows;
    private JavaStreamingContext javaStreamingContext;
    private EventDao dao;
    private SparkSession session;

    @Before
    public void setup(){
        topics = Arrays.asList("partners-events-topic");
        kafkaprops = ImmutableMap.<String, Object>builder()
                .put("bootstrap.servers", "localhost:9092")
                .put("key.deserializer", StringDeserializer.class)
                .put("value.deserializer", StringDeserializer.class)
                .put("group.id", "bot-buster-consumers")
                .put("offsets.autocommit.enable", false)
                .build();

        javaStreamingContext = new JavaStreamingContext("local[*]", "stopbotUnittests", Duration.apply(3));

        rows = session.read().option("header",true).text("./input/dataset").cache();

        dao = new EventDao(javaStreamingContext.sparkContext().sc());

        processor = new KafkaSinkEventStreamProcessor(topics,kafkaprops, javaStreamingContext,dao);
    }

    @Test
    public void shouldAggregateFilterAndFindOneBot(){
        Dataset<Event> messages = rows.map(row -> JsonConverter.fromJson(row.getString(0)), Encoders.bean(Event.class));
        Dataset<BotRegistry> result = processor.identifyBots(messages.toJavaRDD().rdd()).cache();

        assertThat(result.count()).isEqualTo(1);
        assertThat(result.first().getIp()).isEqualTo("148.67.43.14");
    }

    @Test
    @Ignore
    public void testProcessing(){
        processor.process(false);
    }

    @After
    public void cleanUp(){
        session.close();
    }

}