package com.gridu.persistence.ignite;

import com.gridu.model.BotRegistry;
import com.gridu.spark.helpers.SparkArtifactsHelper;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static com.gridu.utils.StopBotUtils.*;

public class IgniteBotRegistryStrategyTest {

    private static IgniteBotRegistryStrategy igniteService;
    private static JavaSparkContext sc;

    private static Ignite ignite;

    @BeforeClass
    public static void setup() {
        startIgnite();
        sc = SparkArtifactsHelper.createSparkContext("local[*]", "botregistrydaotest");
        igniteService = new IgniteBotRegistryStrategy(new JavaIgniteContext(sc, IgniteConfiguration::new));
        sc.setLogLevel("ERROR");
    }

    private static void startIgnite() {
        ignite = Ignition.getOrStart(new IgniteConfiguration());
    }

    @Test
    public void shouldCreateTableAndPersistBotInBlackList() {
        igniteService.persist(sc.parallelize(createBotsList()));

        assertThat(doesTableExists(IgniteBotRegistryStrategy.BOT_REGISTRY_TABLE)).isTrue();

    }

    @Test
    public void shouldSelectAllBotsFromBlacklist() {
        igniteService.persist(sc.parallelize(createBotsList()));
        List<BotRegistry> allBots = igniteService.getAllRecords();
        assertThat(allBots).hasSize(4);
    }

    @Test
    public void shouldReadDatasetFromIgnite() {
        Dataset<BotRegistry> botRegistryDataset = igniteService.loadFromIgnite();
        assertThat(botRegistryDataset.count()).isEqualTo(botRegistryDataset.count());
    }

    @NotNull
    private List<BotRegistry> createBotsList() {
        return Arrays.asList(new BotRegistry("123.456", 5000 ,3,6,2,5),
                new BotRegistry("789.987", 10000 ,3,6,2,5));
    }

    @AfterClass
    public static void cleanUp() {
        ignite.getOrCreateCache(IgniteBotRegistryStrategy.BOT_REGISTRY_CACHE).destroy();
        igniteService.cleanUp();
        ignite.close();
        sc.close();
    }
}

