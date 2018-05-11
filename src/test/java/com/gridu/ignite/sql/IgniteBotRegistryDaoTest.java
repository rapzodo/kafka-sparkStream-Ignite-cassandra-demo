package com.gridu.ignite.sql;
import com.gridu.model.BotRegistry;
import com.gridu.spark.helpers.SparkArtifactsHelper;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.junit.*;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class IgniteBotRegistryDaoTest {

    private  static IgniteBotRegistryDao igniteDao;
    private  static JavaSparkContext sc;

    @BeforeClass
    public static void setup(){
        startIgnite();
        sc = SparkArtifactsHelper.createSparkContext("local[*]", "botregistrydaotest");
        igniteDao = new IgniteBotRegistryDao(new JavaIgniteContext(sc, IgniteConfiguration::new));
        sc.setLogLevel("ERROR");
    }

//    @After
//    public void clearCache() {
//        Ignition.ignite().cache(IgniteBotRegistryDao.BOTREGISTRY_CACHE).clear();
//    }

    private static void startIgnite() {
        Ignition.start();
        Ignition.setClientMode(true);
    }

    private JavaRDD<BotRegistry> getBotRegistryRdd() {
        return createBotRegistryDataSet().toJavaRDD();
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
    public void shouldSelectAllBotsFromBlacklist(){
        Dataset<BotRegistry> botRegistryDataSet = createBotRegistryDataSet();
        igniteDao.persist(botRegistryDataSet);
        List<BotRegistry> allBots = igniteDao.getAllRecords();
        assertThat(allBots).hasSize(4);
    }

    @Test
    public void shouldReadDatasetFromIgnite(){
        Dataset<BotRegistry> botRegistryDataset = igniteDao.loadFromIgnite();
        assertThat(botRegistryDataset.count()).isEqualTo(botRegistryDataset.count());
    }

    private  Dataset<BotRegistry> createBotRegistryDataSet() {
        SparkSession session = SparkSession.builder().sparkContext(sc.sc()).getOrCreate();
        return session.createDataset(createBotsList(), Encoders.bean(BotRegistry.class));
    }

    @NotNull
    private  List<BotRegistry> createBotsList() {
        return Arrays.asList(new BotRegistry("123.456", "http://imabot", 5000),
                new BotRegistry("789.987", "http://imabot", 10000));
    }

    @AfterClass
    public static void cleanUp(){
        Ignition.stopAll(true);
        igniteDao.closeResource();
        sc.close();
    }
}

