import com.gridu.converters.JsonEventMessageConverter;
import com.gridu.spark.sql.IgniteEventDao;
import com.gridu.model.Event;
import com.gridu.spark.helpers.SparkArtifactsHelper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class IgniteEventDaoTest {

    private IgniteEventDao igniteDao;
    private JavaSparkContext sc;

    @Before
    public void setup(){
        sc = SparkArtifactsHelper.createSparkContext("local[*]", "ignitedaotest");
        igniteDao = new IgniteEventDao(sc);
    }

    @Test
    public void shouldReturnOneBot(){
        sc.setLogLevel("ERROR");
        JavaRDD<Event> eventsRDD = sc.textFile("input/dataset")
                .map(jsonString-> JsonEventMessageConverter.fromJson(jsonString));
        assertThat(igniteDao.findBots(eventsRDD,18).count()).isEqualTo(1);
    }

}
