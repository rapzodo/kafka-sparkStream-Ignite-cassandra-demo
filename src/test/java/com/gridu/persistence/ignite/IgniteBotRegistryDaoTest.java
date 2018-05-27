package com.gridu.persistence.ignite;

import com.gridu.model.BotRegistry;
import com.gridu.spark.helpers.SparkArtifactsHelper;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import static com.gridu.utils.StopBotIgniteUtils.*;

import static org.assertj.core.api.Assertions.assertThat;

public class IgniteBotRegistryDaoTest {

    private static IgniteBotRegistryDao igniteDao;
    private static JavaSparkContext sc;

    private static Ignite ignite;

    @BeforeClass
    public static void setup() {
        startIgnite();
        sc = SparkArtifactsHelper.createSparkContext("local[*]", "botregistrydaotest");
        igniteDao = new IgniteBotRegistryDao(new JavaIgniteContext(sc, IgniteConfiguration::new));
        sc.setLogLevel("ERROR");
        Logger.getLogger("org.apache.ignite").setLevel(Level.ERROR);
        ignite.cache(IgniteBotRegistryDao.BOTREGISTRY_CACHE).destroy();
    }

    private static void startIgnite() {
        ignite = startIgniteForTests();
    }

    private JavaRDD<BotRegistry> getBotRegistryRdd() {
        return createBotRegistryDataSet().toJavaRDD();
    }

    @Test
    public void shouldCreateTableAndPersistBotInBlackList() {
        Dataset<BotRegistry> bots = createBotRegistryDataSet();
        igniteDao.persist(bots);

        assertThat(doesTableExists(IgniteBotRegistryDao.BOTREGISTRY_TABLE)).isTrue();

    }

    @Test
    public void shouldSaveAllJavaRddToIgniteRDD() {
        JavaRDD<BotRegistry> botRegistryRdd = getBotRegistryRdd();
        JavaIgniteRDD<Long, BotRegistry> igniteRdd = igniteDao.createAnSaveIgniteRdd(botRegistryRdd);
        assertThat(igniteRdd.count()).isEqualTo(botRegistryRdd.count());
    }

    @Test
    public void shouldSelectBotsDsFromJavaRdd() {
        JavaIgniteRDD<Long, BotRegistry> igniteRdd = igniteDao.createAnSaveIgniteRdd(getBotRegistryRdd());
        Dataset<BotRegistry> botRegistryDataset = igniteDao.getDataSetFromIgniteJavaRdd(igniteRdd);
        assertThat(botRegistryDataset.count()).isEqualTo(igniteRdd.count());
    }

    @Test
    public void shouldSelectAllBotsFromBlacklist() {
        ignite.getOrCreateCache(IgniteBotRegistryDao.BOTREGISTRY_CACHE);
        Dataset<BotRegistry> botRegistryDataSet = createBotRegistryDataSet();
        igniteDao.persist(botRegistryDataSet);
        List<BotRegistry> allBots = igniteDao.getAllRecords();
        assertThat(allBots).hasSize(4);
    }

    @Test
    public void shouldReadDatasetFromIgnite() {
        Dataset<BotRegistry> botRegistryDataset = igniteDao.loadFromIgnite();
        assertThat(botRegistryDataset.count()).isEqualTo(botRegistryDataset.count());
    }

    private Dataset<BotRegistry> createBotRegistryDataSet() {
        SparkSession session = SparkSession.builder().sparkContext(sc.sc()).getOrCreate();
        return session.createDataset(createBotsList(), Encoders.bean(BotRegistry.class));
    }

    @NotNull
    private List<BotRegistry> createBotsList() {
        return Arrays.asList(new BotRegistry("123.456", "http://imabot", 5000),
                new BotRegistry("789.987", "http://imabot", 10000));
    }

    @AfterClass
    public static void cleanUp() {
        igniteDao.cleanUp();
        ignite.close();
        sc.close();
    }
}

