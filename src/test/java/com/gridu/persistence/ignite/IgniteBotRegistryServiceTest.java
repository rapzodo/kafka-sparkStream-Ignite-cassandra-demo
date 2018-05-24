package com.gridu.persistence.ignite;

import com.gridu.model.BotRegistry;
import com.gridu.spark.helpers.SparkArtifactsHelper;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spark.JavaIgniteContext;
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

import static org.assertj.core.api.Assertions.assertThat;
import static com.gridu.utils.StopBotIgniteUtils.*;

public class IgniteBotRegistryServiceTest {

    private static IgniteBotRegistryService igniteService;
    private static JavaSparkContext sc;

    private static Ignite ignite;

    @BeforeClass
    public static void setup() {
        startIgnite();
        sc = SparkArtifactsHelper.createSparkContext("local[*]", "botregistrydaotest");
        igniteService = new IgniteBotRegistryService(new JavaIgniteContext(sc, IgniteConfiguration::new));
        sc.setLogLevel("ERROR");
    }

    private static void startIgnite() {
        ignite = Ignition.getOrStart(new IgniteConfiguration());
    }

    private JavaRDD<BotRegistry> getBotRegistryRdd() {
        return createBotRegistryDataSet().toJavaRDD();
    }

    @Test
    public void shouldCreateTableAndPersistBotInBlackList() {
        Dataset<BotRegistry> bots = createBotRegistryDataSet();
        igniteService.persist(bots);

        assertThat(doesTableExists(IgniteBotRegistryService.BOT_REGISTRY_TABLE)).isTrue();

    }

    @Test
    public void shouldSelectAllBotsFromBlacklist() {
        Dataset<BotRegistry> botRegistryDataSet = createBotRegistryDataSet();
        igniteService.persist(botRegistryDataSet);
        List<BotRegistry> allBots = igniteService.getAllRecords();
        assertThat(allBots).hasSize(4);
    }

    @Test
    public void shouldReadDatasetFromIgnite() {
        Dataset<BotRegistry> botRegistryDataset = igniteService.loadFromIgnite();
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
        igniteService.cleanUp();
        ignite.close();
        sc.close();
    }
}

