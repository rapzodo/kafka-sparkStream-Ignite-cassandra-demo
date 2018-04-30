import com.google.common.collect.ImmutableMap;
import com.gridu.stopbot.converters.JsonConverter;
import com.gridu.stopbot.model.Event;
import com.gridu.stopbot.model.BotRegistry;
import com.gridu.stopbot.spark.processors.KafkaSinkEventStreamProcessor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.*;

import java.util.*;

import static org.assertj.core.api.Assertions.*;

public class KafkaSinkEventStreamProcessorTest{
    private KafkaSinkEventStreamProcessor processor;
    private SparkSession session;
    private List<String> topics;
    private Map<String,Object> kafkaprops;
    private Dataset<Row> rows;
    private JavaStreamingContext javaStreamingContext;

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

        session = SparkSession.builder().sparkContext(javaStreamingContext.sparkContext().sc()).getOrCreate();
        session.sparkContext().setLogLevel("ERROR");
        rows = session.read().option("header",true).text("./input/dataset").cache();

        processor = new KafkaSinkEventStreamProcessor(topics,kafkaprops, javaStreamingContext);
    }

    @Test
    public void testIpIUrlActionsAggregation(){
        Dataset<Event> messages = rows.map(row -> JsonConverter.fromJson(row.getString(0)), Encoders.bean(Event.class));
        Dataset<BotRegistry> result = processor.aggregateAndCountIpUrlActions(messages);
        BotRegistry expected = new BotRegistry("148.67.43.14",
                "http://9d345009-a-62cb3a1a-s-sites.googlegroups.com/index.html",19);
        assertThat(result.first()).isEqualTo(expected);
    }

    @Test
    public void shouldAggregateFilterAndFindOneBot(){
        Dataset<Event> messages = rows.map(row -> JsonConverter.fromJson(row.getString(0)), Encoders.bean(Event.class));
        Dataset<BotRegistry> result = processor.findBots(messages).cache();

        assertThat(result.count()).isEqualTo(1);
        assertThat(result.first().getIp()).isEqualTo("148.67.43.14");
    }

    @Test
    public void testProcessing(){
        processor.process(false);
    }

    @After
    public void cleanUp(){
        session.close();
    }

}
