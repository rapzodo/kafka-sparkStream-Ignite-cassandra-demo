package com.gridu.persistence.ignite;

import com.gridu.converters.JsonEventMessageConverter;
import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import com.gridu.spark.helpers.SparkArtifactsHelper;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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
        eventJavaRDD = loadEventMessagesRdd(null);
        igniteEventStrategy.cleanUp();
    }

    private static void startIgnite() {
        ignite = Ignition.getOrStart(new IgniteConfiguration());
    }

    private static JavaRDD<Event> loadEventMessagesRdd(String filePath) {
        if(filePath == null){
            filePath = "input/dataset.txt";
        }
        return sc.textFile(filePath)
//                .sample(false,0.1,1)
                .map(JsonEventMessageConverter::fromJson)
                .cache();
    }

    @Test
    public void shouldCreateTableAndPersistEventsToIgnite() {
        igniteEventStrategy.persist(eventJavaRDD);

        getTables().show();

        assertThat(doesTableExists(IgniteEventStrategy.EVENT_TABLE)).isTrue();
        assertThat(igniteEventStrategy.loadFromCache().count()).isEqualTo(eventJavaRDD.count());
    }


    @Test
    public void shouldRetrieveViewsAndClicksDifferenceByIp() {
        final Dataset baseDs = cleanPersistAndLoadBaseDsFromCache(eventJavaRDD);

        final Dataset<Row> viewsClicksDiffByIp = igniteEventStrategy.fetchViewsAndClicksDifferenceByIp(baseDs);
        viewsClicksDiffByIp
                .show();

        assertThat(viewsClicksDiffByIp.first().get(3)).isEqualTo(5.0);
    }

    @Test
    public void shouldRetrieveEventsCountByIp(){
        final Dataset<Row> dataset = cleanPersistAndLoadBaseDsFromCache(eventJavaRDD);
        final Dataset<Row> ipsCount = igniteEventStrategy.fetchIpEventsCount(dataset);
        ipsCount.show();
        assertThat(ipsCount.first().getLong(1)).isEqualTo(8);
    }

    @Test
    public void shouldRetrieveNumberOfDifferentCategoriesByIp(){
        final Dataset<Row> categoriesByIpCount = igniteEventStrategy
                .fetchCategoriesByIpCount(cleanPersistAndLoadBaseDsFromCache(eventJavaRDD));

        assertThat(categoriesByIpCount.first().getLong(1)).isEqualTo(7);
    }

    @Test
    public void shouldRetrieveBotsCandidatesShortlist(){
        final Dataset<Row> botsShortList = igniteEventStrategy
                .shortListEventsForBotsVerification(cleanPersistAndLoadBaseDsFromCache(eventJavaRDD));
        botsShortList.show();
        final Row candidate = botsShortList.first();
        assertThat(candidate.getLong(1)).isEqualTo(6);//events
        assertThat(candidate.getLong(2)).isEqualTo(5);//views
        assertThat(candidate.getLong(3)).isEqualTo(1);//clicks
        assertThat(candidate.getDouble(4)).isEqualTo(5.0);//diff
        assertThat(candidate.getLong(5)).isEqualTo(5);//categories
    }

    @Test
    public void shouldReturnNoResultsForIdentifyBots(){
        final JavaRDD<Event> eventJavaRDD = loadEventMessagesRdd("input/dataset_sample.txt");
        final Dataset<Row> botsShortList = igniteEventStrategy
                .shortListEventsForBotsVerification(cleanPersistAndLoadBaseDsFromCache(eventJavaRDD));
        final Dataset<BotRegistry> botRegistryDataset = igniteEventStrategy.identifyBots(botsShortList);
        assertThat(botRegistryDataset.count()).isZero();
    }

    @Test
    public void shouldReturnOneResultsForIdentifyBots(){
        final Dataset<Row> botsShortList = igniteEventStrategy
                .shortListEventsForBotsVerification(cleanPersistAndLoadBaseDsFromCache(eventJavaRDD));
        final Dataset<BotRegistry> botRegistryDataset = igniteEventStrategy.identifyBots(botsShortList);
        assertThat(botRegistryDataset.count()).isOne();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenNoColumnsAreProvided(){
        igniteEventStrategy.selectAggregateAndCount(ic
                .fromCache(igniteEventStrategy.getBotRegistryCacheConfiguration()), IgniteEventStrategy.EVENT_TABLE);
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

    private Dataset<Row> cleanPersistAndLoadBaseDsFromCache(JavaRDD<Event> eventJavaRDD) {
        clearEventsCache();

        igniteEventStrategy.persist(eventJavaRDD);

        return igniteEventStrategy.loadFromCache();
    }
}

