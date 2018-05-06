import com.gridu.converters.JsonEventMessageConverter;
import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import com.gridu.spark.helpers.SparkArtifactsHelper;
import com.gridu.spark.sql.SparkSQLEventDao;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SparkSqlEventDaoTest {

    private SparkSQLEventDao dao;
    private JavaRDD<String> lines;

    @Before
    public void setup(){

        String master = "local[*]";
        String appName = "eventdaoUnitTest";

        JavaSparkContext sparkContext = SparkArtifactsHelper.createSparkContext(master, appName);
        lines = sparkContext
                .textFile("./input/dataset")
                .cache();
        dao = new SparkSQLEventDao(sparkContext.sc());
        dao.setLoggerLevel("ERROR");
    }

    @Test
    public void shouldReturnAllValidJsonMessagesAsEvents(){
        assertThat(dao.getEventDataSet(mapToEventDataSet()).count()).isEqualTo(648556L);
    }

    @Test
    public void shouldAggregateFilterAndFindOneBot(){
        JavaRDD<Event> messages = mapToEventDataSet();
        Dataset<BotRegistry> result = dao.findBots(messages,18).cache();

        BotRegistry expected = new BotRegistry("148.67.43.14",
                "http://9d345009-a-62cb3a1a-s-sites.googlegroups.com/index.html",19);
        assertThat(result.count()).isEqualTo(1);
        assertThat(result.first()).isEqualTo(expected);
    }

    private JavaRDD<Event> mapToEventDataSet() {
        return lines.map(line -> JsonEventMessageConverter.fromJson(line));
    }
}
