package com.gridu.ignite.sql;
import com.gridu.ignite.sql.IgniteBotRegistryDao;
import com.gridu.ignite.sql.IgniteDao;
import com.gridu.model.BotRegistry;
import com.gridu.spark.helpers.SparkArtifactsHelper;
import org.apache.ignite.Ignition;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class IgniteBotRegistryDaoTest {

    private static IgniteBotRegistryDao igniteDao;
    private static JavaSparkContext sc;
    private static JavaRDD<BotRegistry> botRegistryJavaRDD;
    private static JavaIgniteRDD<Long, BotRegistry> igniteRdd;
    private static Dataset<BotRegistry> botRegistryDataset;

    @BeforeClass
    public static void setup() {
        startIgnite();
        sc = SparkArtifactsHelper.createSparkContext("local[*]", "botregistrydaotest");
        igniteDao = new IgniteBotRegistryDao(sc);
        loadEventMessagesRdd();
        sc.setLogLevel("ERROR");
    }

    private static void startIgnite() {
        Ignition.start();
        Ignition.setClientMode(true);
    }

    private static void loadEventMessagesRdd() {
        botRegistryJavaRDD = createBotRegistryDataSet().toJavaRDD();
    }

    @Test
    public void shouldCreateTableAndPersistBotInBlackList() {
        Dataset<BotRegistry> bots = createBotRegistryDataSet();
        igniteDao.persist(bots);

        assertThat(IgniteDao.getDataTables().first().name())
                .isEqualTo(IgniteBotRegistryDao.BOTREGISTRY_TABLE);

    }

    @Test
    public void shouldSaveAllJavaRddToIgniteRDD() {
        igniteRdd = igniteDao.createAnSaveIgniteRdd(botRegistryJavaRDD);
        assertThat(igniteRdd.count()).isEqualTo(botRegistryJavaRDD.count());
    }

    @Test
    public void shouldSelectBotsDsFromJavaRdd() {
        botRegistryDataset = igniteDao.getDataSetFromJavaRdd(igniteRdd);
        assertThat(botRegistryDataset.count()).isEqualTo(igniteRdd.count());
    }

    @Test
    public void shouldSelectAllBotsFromBlacklist(){
        List<BotRegistry> allBots = igniteDao.getAllRecords();
        assertThat(allBots).hasSize(2);
        assertThat(allBots.get(0).getIp()).isEqualTo("123.456");
    }

    @Test
    public void shouldReadDatasetFromIgnite(){
        Dataset<BotRegistry> botRegistryDataset = igniteDao.loadFromIgnite();
        assertThat(botRegistryDataset).isNotNull();
    }

    private static Dataset<BotRegistry> createBotRegistryDataSet() {
        SparkSession session = SparkSession.builder().sparkContext(sc.sc()).getOrCreate();
        return session.createDataset(createBotsList(), Encoders.bean(BotRegistry.class));
    }

    @NotNull
    private static List<BotRegistry> createBotsList() {
        return Arrays.asList(new BotRegistry("123.456", "http://imabot", 5000),
                new BotRegistry("789.987", "http://imabot", 10000));
    }

    @AfterClass
    public static void cleanUp() {
        igniteDao.closeResource();
        Ignition.stop(true);
    }
}

