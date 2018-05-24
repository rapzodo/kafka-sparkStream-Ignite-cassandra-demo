package com.gridu.persistence.ignite;

import com.gridu.converters.JsonEventMessageConverter;
import com.gridu.model.Event;
import com.gridu.spark.helpers.SparkArtifactsHelper;
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
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static com.gridu.utils.StopBotIgniteUtils.*;

public class IgniteEventServiceTest {

    private static IgniteEventService igniteEventService;
    private static JavaSparkContext sc;
    private static JavaRDD<Event> eventJavaRDD;
    private static JavaIgniteContext ic;
    private static Ignite ignite;

    @BeforeClass
    public static void setup() {
        startIgnite();
        sc = SparkArtifactsHelper.createSparkContext("local[*]", "igniteeventdaotest");
        ic = new JavaIgniteContext(sc,IgniteConfiguration::new);
        igniteEventService = new IgniteEventService(ic);
        sc.setLogLevel("ERROR");
        eventJavaRDD = loadEventMessagesRdd();
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
        igniteEventService.persist(events);

        getTables().show();
        assertThat(doesTableExists(IgniteEventService.EVENT_TABLE)).isTrue();
    }

    @Test
    public void shouldSaveAllJavaRddToIgniteRDD() {
        JavaIgniteRDD<Long, Event> igniteRdd = igniteEventService.createAnSaveIgniteRdd(sc.parallelize(createEventsList()));
        assertThat(igniteRdd.count()).isEqualTo(1);
    }

    @Test
    public void shouldSqlEventsDsFromJavaRdd() {
        clearEventsCache();
        JavaIgniteRDD<Long, Event> igniteRDD = igniteEventService.createAnSaveIgniteRdd(sc.parallelize(createEventsList()));
        Dataset<Event> dataSetFromJavaRdd = igniteEventService.getDataSetFromIgniteJavaRdd(igniteRDD);
        assertThat(igniteRDD.count()).isEqualTo(1);
        assertThat(dataSetFromJavaRdd.count()).isEqualTo(igniteRDD.count());
    }


    @Test
    public void shouldAggregateAndCountIpUrlActionsAndOrderByDesc() {
        final Dataset<Row> aggregatedEvents = igniteEventService
                .saveAggregateAndCountEvents(sc.parallelize(createEventsList()));
        aggregatedEvents.show(false);
        assertThat(aggregatedEvents.first().get(2)).isEqualTo(19L);
    }

    private JavaIgniteRDD<Long, Event> getJavaIgniteRDD() {
        return igniteEventService.createAnSaveIgniteRdd(sc.parallelize(createEventsList()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenNoColumnsAreProvided(){
        igniteEventService.selectAggregateAndCount(getJavaIgniteRDD(), IgniteEventService.EVENT_TABLE);
    }

    @Test
    public void shouldSelectAllEventsFromEventTable(){
        clearEventsCache();
        igniteEventService.createAnSaveIgniteRdd(sc.parallelize(createEventsList()));
        List<Event> events = igniteEventService.getAllRecords();
        assertThat(events).hasSize(1);
        assertThat(events.get(0).getIp()).isEqualTo("123.345");
    }

    @NotNull
    private List<Event> createEventsList() {
        return Arrays.asList(new Event("click", "123.345", new Date().getTime(), "http://stopbot.com"));
    }

    private void clearEventsCache() {
        ignite.cache(IgniteEventService.EVENTS_CACHE_NAME).clear();
    }

    @AfterClass
    public static void cleanUp() {
        igniteEventService.cleanUp();
        ignite.close();
        sc.close();
    }
}

