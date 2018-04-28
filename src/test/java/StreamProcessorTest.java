import com.gridu.stopbot.model.Event;
import com.gridu.stopbot.model.QueryResult;
import com.gridu.stopbot.spark.processing.StreamProcessor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.*;

public class StreamProcessorTest {
    private StreamProcessor processor;
    private SparkSession session;

    @Before
    public void setup(){
        session = SparkSession.builder().appName("stopbot").master("local[*]").getOrCreate();
        session.sparkContext().setLogLevel("ERROR");
        processor = new StreamProcessor(Collections.emptyList(),new HashMap<>(), session);
    }

    @Test
    public void shouldReturnTrueWhenExceedingClicks(){
        assertThat(processor.exceedClicks(new QueryResult("123.345","http://someurl",11))).isTrue();
    }

    @Test
    public void shouldReturn2IpUrlCountAggregation(){
        Dataset<Row> rows = session.read().option("header",true).text("./input/dataset.txt");
        Dataset<Event> messages = rows.map(row -> Event.fromJson(row.getString(0)), Encoders.bean(Event.class));

        assertThat(rows.count()).isEqualTo(3);

        Dataset<QueryResult> result = processor.countUrlActions(messages.toJavaRDD());

        assertThat(result.first().getNumberOfActions()).isEqualTo(2);
    }

}
