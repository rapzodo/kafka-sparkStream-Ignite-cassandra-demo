import com.gridu.converters.JsonEventMessageConverter;
import com.gridu.ignite.sql.IgniteBlacklistDao;
import com.gridu.ignite.sql.IgniteEventDao;
import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import com.gridu.spark.helpers.SparkArtifactsHelper;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class IgniteEventDaoTest {

    private static IgniteEventDao igniteDao;
    private static JavaSparkContext sc;
    private static JavaRDD<Event> eventsRDD;
    private static JavaIgniteRDD<Long, Event> igniteRdd;
    private static Dataset<Event> eventDataset;

    @BeforeClass
    public static void setup() {
        startIgnite();
        sc = SparkArtifactsHelper.createSparkContext("local[*]", "ignitedaotest");
        igniteDao = new IgniteEventDao(sc);
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
    public void sholdSaveAllJavaRddToIgniteRDD() {
        igniteRdd = igniteDao.createAnSaveIgniteRdd(eventsRDD);
        assertThat(igniteRdd.count()).isEqualTo(eventsRDD.count());
    }

    @Test
    public void shouldSqlEventsDsFromJavaRdd() {
        eventDataset = igniteDao.getEventsDataSetFromJavaRdd(igniteRdd);
        assertThat(eventDataset.count()).isEqualTo(igniteRdd.count());
    }

    @Test
    public void shouldAggregateAndCountIpUrlActionsAndOrderByDesc(){
        Dataset<Row> bots = igniteDao.aggregateAndCountUrlActionsByIp(igniteRdd);
        assertThat(bots.first().get(2)).isEqualTo(19L);
    }

    @Test
    public void shouldIdentifyAndReturnOneBot(){
        Dataset<Row> botRegistryDataset = igniteDao.aggregateAndCountUrlActionsByIp(igniteRdd);
        Dataset<BotRegistry> bots = igniteDao.identifyBots(botRegistryDataset,18);
        assertThat(bots.first().getCount()).isEqualTo(19);
        assertThat(bots.first().getIp()).isEqualTo("148.67.43.14");
    }

    @Test
    public void shouldPersistBotInBlackList(){
        Dataset<Row> botRegistryDataset = igniteDao.aggregateAndCountUrlActionsByIp(igniteRdd);
        Dataset<BotRegistry> bots = igniteDao.identifyBots(botRegistryDataset,18);
        new IgniteBlacklistDao().persist(bots);
        IgniteCache<Long, BotRegistry> blacklist = Ignition.ignite().cache("BLACKLIST");
        igniteDao.getDataTables().show();
        List<List<?>> all = blacklist.query(new SqlFieldsQuery("select * from BLACKLIST")).getAll();
        all.forEach(objects -> System.out.println(objects));
    }

    @AfterClass
    public static void cleanUp() {
        igniteDao.closeResource();
        Ignition.stop(true);
    }
}

