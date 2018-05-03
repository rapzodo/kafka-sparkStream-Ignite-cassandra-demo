import com.gridu.converters.JsonEventMessageConverter;
import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import com.gridu.spark.helpers.SparkArtifactsHelper;
import com.gridu.spark.sql.EventDao;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class EventDaoTest {

    private EventDao dao;
    private Dataset<Row> rows;

    @Before
    public void setup(){

        String master = "local[*]";
        String appName = "eventdaoUnitTest";
        SparkSession session = SparkArtifactsHelper.createSparkSession(master,appName);

        rows = session.read().option("header",true)
                .text("./input/dataset").cache();
        dao = new EventDao(session);
        dao.setLoggerLevel("ERROR");
    }

    @Test
    public void shouldAggregateFilterAndFindOneBot(){
        Dataset<Event> messages = rows.map(row -> JsonEventMessageConverter.fromJson(row.getString(0)), Encoders.bean(Event.class));
        Dataset<BotRegistry> result = dao.findBots(messages.toJavaRDD(),18).cache();

        BotRegistry expected = new BotRegistry("148.67.43.14",
                "http://9d345009-a-62cb3a1a-s-sites.googlegroups.com/index.html",19);
        assertThat(result.count()).isEqualTo(1);
        assertThat(result.first()).isEqualTo(expected);
    }
}
