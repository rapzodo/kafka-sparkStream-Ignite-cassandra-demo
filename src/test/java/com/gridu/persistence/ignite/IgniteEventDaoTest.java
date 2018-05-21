package com.gridu.persistence.ignite;

import com.gridu.converters.JsonEventMessageConverter;
import com.gridu.model.Event;
import com.gridu.spark.helpers.SparkArtifactsHelper;
import com.gridu.spark.utils.IgniteUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class IgniteEventDaoTest {

    private static IgniteEventDao igniteDao;
    private static JavaSparkContext sc;
    private static JavaRDD<Event> eventJavaRDD;
    private static JavaIgniteContext ic;
    private static Ignite ignite;

    @BeforeClass
    public static void setup() {
        startIgnite();
        sc = SparkArtifactsHelper.createSparkContext("local[*]", "igniteeventdaotest");
        ic = new JavaIgniteContext(sc,IgniteConfiguration::new);
        igniteDao = new IgniteEventDao(ic);
        sc.setLogLevel("ERROR");
        eventJavaRDD = loadEventMessagesRdd();
        destroyCache();
    }

    private static void destroyCache() {
        ignite.cache(IgniteEventDao.EVENTS_CACHE_NAME).destroy();
    }

    private static void startIgnite() {
        ignite = Ignition.getOrStart(new IgniteConfiguration());
    }

    private static JavaRDD<Event> loadEventMessagesRdd() {
        return sc.textFile("input/dataset")
                .map(JsonEventMessageConverter::fromJson).cache();
    }

    @Test
    public void shouldCreateTableAndPersistEventsToIgnite() {
        Dataset<Event> events = SparkSession.builder().sparkContext(sc.sc()).getOrCreate()
                .createDataset(eventJavaRDD.take(5), Encoders.bean(Event.class));
        igniteDao.persist(events);

        IgniteUtils.getTables().show();
        assertThat(IgniteUtils.doesTableExists(IgniteEventDao.EVENT_TABLE)).isTrue();
    }

    @Test
    public void shouldSaveAllJavaRddToIgniteRDD() {
        JavaIgniteRDD<Long, Event> igniteRdd = igniteDao.createAnSaveIgniteRdd(sc.parallelize(createEventsList()));
        assertThat(igniteRdd.count()).isEqualTo(1);
    }

    @Test
    public void shouldSqlEventsDsFromJavaRdd() {
        clearEventsCache();
        JavaIgniteRDD<Long, Event> igniteRDD = igniteDao.createAnSaveIgniteRdd(sc.parallelize(createEventsList()));
        Dataset<Event> dataSetFromJavaRdd = igniteDao.getDataSetFromIgniteJavaRdd(igniteRDD);
        assertThat(igniteRDD.count()).isEqualTo(1);
        assertThat(dataSetFromJavaRdd.count()).isEqualTo(igniteRDD.count());
    }


    @Test
    public void shouldAggregateAndCountIpUrlActionsAndOrderByDesc() {
        JavaIgniteRDD<Long, Event> igniteRDD = igniteDao.createAnSaveIgniteRdd(eventJavaRDD);
        Dataset<Row> aggregatedDS = igniteDao.aggregateAndCount(igniteDao.getDataSetFromIgniteJavaRdd(igniteRDD),
                functions.col("ip"),functions.col("url")).cache();
        aggregatedDS.show(false);
        assertThat(aggregatedDS.first().get(2)).isEqualTo(19L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenNoColumnsAreProvided(){
        final Dataset<Event> eventDataset = SparkArtifactsHelper.createSparkSession(sc)
                .createDataset(Collections.singletonList(new Event()), Encoders.bean(Event.class));
        igniteDao.aggregateAndCount(eventDataset);
    }
    @Test
    public void shouldSelectAllEventsFromEventTable(){
        clearEventsCache();
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

    private void clearEventsCache() {
        ignite.cache(IgniteEventDao.EVENTS_CACHE_NAME).clear();
    }

    @AfterClass
    public static void cleanUp() {
        igniteDao.cleanUp();
        ignite.close();
        sc.close();
    }
}

