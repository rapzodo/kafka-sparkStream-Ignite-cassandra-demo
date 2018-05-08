import com.gridu.ignite.sql.IgniteDao;
import com.gridu.converters.JsonEventMessageConverter;
import com.gridu.ignite.sql.IgniteEventDao;
import com.gridu.model.Event;
import com.gridu.spark.helpers.SparkArtifactsHelper;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.junit.*;

import static org.assertj.core.api.Assertions.assertThat;

public class IgniteEventDaoTest {

    private static IgniteDao<Long, Event> igniteDao;
    private static JavaSparkContext sc;
    private static JavaRDD<Event> eventsRDD;
    private static JavaIgniteRDD<Long, Event> igniteRdd;

    @BeforeClass
    public static void setup() {
        Ignition.start(IgniteEventDao.CONFIG_FILE);
        Ignition.setClientMode(true);
        sc = SparkArtifactsHelper.createSparkContext("local[*]", "ignitedaotest");
        igniteDao = new IgniteEventDao(new JavaIgniteContext(sc, "config/example-shared-rdd.xml"));
        eventsRDD = sc.textFile("input/dataset")
                .map(jsonString -> JsonEventMessageConverter.fromJson(jsonString)).cache();
        sc.setLogLevel("ERROR");
//        igniteDao.saveDataset(eventsRDD);
    }


    @Test
    public void sholdSaveAllJavaRddToIgniteRDD() {
        igniteRdd = igniteDao.createAnSaveIgniteRdd(eventsRDD);
        assertThat(igniteRdd.count()).isEqualTo(eventsRDD.count());
    }

    @Test
    public void shouldSqlEventsDsFromJavaRdd() {
        Dataset<Event> eventDataset = igniteDao.getEventsDataSetFromJavaRdd(igniteRdd);
        assertThat(eventDataset.count()).isEqualTo(igniteRdd.count());
    }

    @AfterClass
    public static void cleanUp() {
        igniteDao.closeResource();
        Ignition.stop(true);
    }
}

