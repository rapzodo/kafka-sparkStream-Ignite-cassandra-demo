package com.gridu.ignite.sql;

import com.gridu.converters.JsonEventMessageConverter;
import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import com.gridu.spark.helpers.SparkArtifactsHelper;
import org.apache.ignite.Ignition;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class IgniteEventDaoTest {

    private static IgniteEventDao igniteDao;
    private static JavaSparkContext sc;
    private static JavaRDD<Event> eventJavaRDD;

    @BeforeClass
    public static void setup() {
        startIgnite();
        sc = SparkArtifactsHelper.createSparkContext("local[*]", "igniteeventdaotest");
        igniteDao = new IgniteEventDao(sc);
        sc.setLogLevel("ERROR");
        eventJavaRDD = loadEventMessagesRdd();
    }

    private static void startIgnite() {
        Ignition.start();
        Ignition.setClientMode(true);
    }

    private static JavaRDD<Event> loadEventMessagesRdd() {
        return sc.textFile("input/dataset")
                .map(JsonEventMessageConverter::fromJson).cache();
    }

    @Test
    public void shouldCreateTableAndPersistBotInBlackList() {
        Dataset<Event> bots = SparkSession.builder().sparkContext(sc.sc()).getOrCreate()
                .createDataset(eventJavaRDD.take(5), Encoders.bean(Event.class));
        igniteDao.persist(bots);

        assertThat(IgniteDao.getDataTables().first().name())
                .isEqualTo(IgniteEventDao.EVENT_TABLE);

    }

    @Test
    public void shouldSaveAllJavaRddToIgniteRDD() {
        JavaIgniteRDD<Long, Event> igniteRdd = igniteDao.createAnSaveIgniteRdd(sc.parallelize(createEventsList()));
        assertThat(igniteRdd.count()).isEqualTo(1);
    }

    @Test
    public void shouldSqlEventsDsFromJavaRdd() {
        JavaIgniteRDD<Long, Event> igniteRDD = igniteDao.createAnSaveIgniteRdd(sc.parallelize(createEventsList()));
        Dataset<Event> dataSetFromJavaRdd = igniteDao.getDataSetFromJavaRdd(igniteRDD);
        assertThat(igniteRDD.count()).isEqualTo(1);
        assertThat(dataSetFromJavaRdd.count()).isEqualTo(igniteRDD.count());
    }

    @Test
    public void shouldAggregateAndCountIpUrlActionsAndOrderByDesc() {
        JavaIgniteRDD<Long, Event> igniteRDD = igniteDao.createAnSaveIgniteRdd(eventJavaRDD);
        Dataset<Row> bots = igniteDao.aggregateAndCountUrlActionsByIp(igniteDao.getDataSetFromJavaRdd(igniteRDD));
        assertThat(bots.first().get(2)).isEqualTo(19L);
    }
//
    @Test
    public void shouldIdentifyAndReturnOneBot(){
        JavaIgniteRDD<Long, Event> igniteRDD = igniteDao.createAnSaveIgniteRdd(eventJavaRDD);
        Dataset<Row> aggregatedDs = igniteDao.aggregateAndCountUrlActionsByIp(igniteDao.getDataSetFromJavaRdd(igniteRDD));
        Dataset<BotRegistry> bots = igniteDao.identifyBots(aggregatedDs,18);
        assertThat(bots.first().getCount()).isEqualTo(19);
        assertThat(bots.first().getIp()).isEqualTo("148.67.43.14");
    }

    @Test
    public void shouldSelectAllEventsFromEventTable(){
        List<Event> eventsList = createEventsList();
        igniteDao.createAnSaveIgniteRdd(sc.parallelize(eventsList));
        List<Event> events = igniteDao.getAllRecords();
        assertThat(events).hasSize(1);
        assertThat(events.get(0).getIp()).isEqualTo("123.345");
    }

    @NotNull
    private List<Event> createEventsList() {
        return Arrays.asList(new Event("click", "123.345", new Date().getTime(), "http://stopbot.com"));
    }

    @AfterClass
    public static void cleanUp() {
        igniteDao.closeResource();
        Ignition.stop(true);
        sc.close();
    }
}

