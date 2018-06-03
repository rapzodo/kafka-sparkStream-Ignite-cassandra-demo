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
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static com.gridu.utils.StopBotUtils.doesTableExists;
import static com.gridu.utils.StopBotUtils.getTables;
import static org.assertj.core.api.Assertions.assertThat;


public class IgniteEventStrategyTest {

    private static IgniteEventStrategy igniteEventStrategy;
    private static JavaSparkContext sc;
    private static JavaRDD<Event> eventJavaRDD;
    private static JavaIgniteContext ic;
    private static Ignite ignite;

    @BeforeClass
    public static void setup() {
        startIgnite();
        sc = SparkArtifactsHelper.createSparkContext("local[*]", "igniteeventdaotest");
        ic = new JavaIgniteContext(sc,IgniteConfiguration::new);
        igniteEventStrategy = new IgniteEventStrategy(ic);
        sc.setLogLevel("ERROR");
        eventJavaRDD = loadEventMessagesRdd();
    }

    private static void startIgnite() {
        ignite = Ignition.getOrStart(new IgniteConfiguration());
    }

    private static JavaRDD<Event> loadEventMessagesRdd() {
        return sc.textFile("input/dataset.txt")
//                .sample(false,0.1,1)
                .map(JsonEventMessageConverter::fromJson)
                .cache();
    }

    @Test
    public void shouldCreateTableAndPersistEventsToIgnite() throws AnalysisException {
        igniteEventStrategy.persist(eventJavaRDD);

        getTables().show();

        assertThat(doesTableExists(IgniteEventStrategy.EVENT_TABLE)).isTrue();
    }


    @Test//TODO extract fetchMethods to separate tests
    public void shouldAggregateAndCountIpUrlActionsAndOrderByDesc() {
        igniteEventStrategy.cleanUp();

        igniteEventStrategy.persist(eventJavaRDD);

        final Dataset baseDs = igniteEventStrategy.loadFromCache();

        final Dataset ipsCount = igniteEventStrategy.fetchIpEventsCount();
        ipsCount.show();

        final Dataset<Row> viewsClicksDiffByIp = igniteEventStrategy.fetchViewsAndClicksDifferenceByIp(baseDs);
        viewsClicksDiffByIp
                .show();

        final Dataset<Row> categoriesByIp = igniteEventStrategy.fetchCategoriesByIpCount(baseDs);
        categoriesByIp.show();

        igniteEventStrategy.shortListEventsForBotsVerification(baseDs).show();
//        aggregatedEvents.show(false);
//        assertThat(aggregatedEvents.first().get(2)).isEqualTo(5L);
    }

    private JavaIgniteRDD<Long, Event> getJavaIgniteRDD() {
        return igniteEventStrategy.saveIgniteRdd(sc.parallelize(createEventsList()),ic,
                igniteEventStrategy.getBotRegistryCacheConfiguration());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenNoColumnsAreProvided(){
        igniteEventStrategy.selectAggregateAndCount(ic
                .fromCache(igniteEventStrategy.getBotRegistryCacheConfiguration()), IgniteEventStrategy.EVENT_TABLE);
    }

    @Test
    public void shouldSelectAllEventsFromEventTable(){
        clearEventsCache();
        igniteEventStrategy.saveIgniteRdd(sc.parallelize(createEventsList()),ic,
                igniteEventStrategy.getBotRegistryCacheConfiguration());
        List<Event> events = igniteEventStrategy.getAllRecords();
        assertThat(events).hasSize(1);
        assertThat(events.get(0).getIp()).isEqualTo("123.345");
    }

    @NotNull
    private List<Event> createEventsList() {
        return Arrays.asList(new Event("click", "123.345", new Date().getTime(), "http://stopbot.com"));
    }

    private void clearEventsCache() {
        ignite.cache(IgniteEventStrategy.EVENTS_CACHE_NAME).clear();
    }

    @AfterClass
    public static void cleanUp() {
        igniteEventStrategy.cleanUp();
        ignite.close();
        sc.close();
    }
}

