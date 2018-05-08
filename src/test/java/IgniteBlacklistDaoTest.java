import com.gridu.converters.JsonEventMessageConverter;
import com.gridu.ignite.sql.IgniteBlacklistDao;
import com.gridu.ignite.sql.IgniteDao;
import com.gridu.ignite.sql.IgniteEventDao;
import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import com.gridu.spark.helpers.SparkArtifactsHelper;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class IgniteBlacklistDaoTest {

    private static IgniteDao igniteDao;
    private static JavaSparkContext sc;
    private static JavaRDD<Event> eventsRDD;
    private static JavaIgniteRDD<Long, Event> igniteRdd;
    private static Dataset<Event> eventDataset;

    @BeforeClass
    public static void setup() {
        startIgnite();
        sc = SparkArtifactsHelper.createSparkContext("local[*]", "ignitedaotest");
        igniteDao = new IgniteBlacklistDao();
        loadEventMessagesRdd();
        sc.setLogLevel("ERROR");
//        igniteDao.persist(eventsRDD);
    }

    private static void startIgnite() {
        Ignition.start();
        Ignition.setClientMode(true);
    }

    private static void loadEventMessagesRdd() {
        eventsRDD = sc.textFile("input/dataset")
                .map(jsonString -> JsonEventMessageConverter.fromJson(jsonString)).cache();
    }


    @Test
    @Ignore
    public void shouldSelectEventsDsFromJavaRdd() {
        eventDataset = igniteDao.getEventsDataSetFromJavaRdd(igniteRdd);
        assertThat(eventDataset.count()).isEqualTo(igniteRdd.count());
    }

    @Test
    @Ignore
    public void shouldAggregateAndCountIpUrlActionsAndOrderByDesc(){
        Dataset<Row> bots = igniteDao.aggregateAndCountUrlActionsByIp(igniteRdd);
        assertThat(bots.first().get(2)).isEqualTo(19L);
    }

    @Test
    public void shouldPersistBotInBlackList(){

        Dataset<BotRegistry> bots = createBotRegistryDataSet();
        igniteDao.persist(bots);

        IgniteDao.getDataTables().show();
//        List<List<?>> all = blacklist.query(new SqlFieldsQuery("select * from BLACKLIST")).getAll();
//        all.forEach(objects -> System.out.println(objects));
    }

    private Dataset<BotRegistry> createBotRegistryDataSet() {
        SparkSession session = SparkSession.builder().sparkContext(sc.sc()).getOrCreate();
        return session.createDataset(Arrays.asList(new BotRegistry("123.456","http://imabot",5000),
                new BotRegistry("789.987","http://imabot",10000))
                ,Encoders.bean(BotRegistry.class));
    }

    @AfterClass
    public static void cleanUp() {
        igniteDao.closeResource();
        Ignition.stop(true);
    }
}

