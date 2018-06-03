package com.gridu.persistence.ignite;

import com.gridu.model.BotRegistry;
import com.gridu.utils.StopBotUtils;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.ignite.spark.IgniteDataFrameSettings.*;

public class IgniteBotRegistryStrategy implements IgniteStrategy<Long, BotRegistry> {

    private static final Logger logger = LoggerFactory.getLogger(IgniteBotRegistryStrategy.class);

    public static final String BOT_REGISTRY_TABLE = "BOTREGISTRY";
    public static final String BOT_REGISTRY_CACHE = "botRegistryCache";
    private JavaIgniteContext<Long,BotRegistry> ic;
    private CacheConfiguration<Long,BotRegistry> botRegistryCacheConfiguration;
    private IgniteCache<Long,BotRegistry> botsCache;

    public IgniteBotRegistryStrategy(JavaIgniteContext javaIgniteContext) {
        ic = javaIgniteContext;
        setup();
    }

    @Override
    public void setup() {
        botRegistryCacheConfiguration = new CacheConfiguration<>(BOT_REGISTRY_CACHE);
        botRegistryCacheConfiguration.setIndexedTypes(Long.class, BotRegistry.class);
        setExpirePolicy();
        botsCache = ic.ignite().getOrCreateCache(botRegistryCacheConfiguration);
        StopBotUtils.getTables().show();
    }

    @Override
    public void persist(JavaRDD<BotRegistry> botRegistryJavaRDD) {
        logger.info(">>>SAVING BOTS TO IGNITE BLACKLIST<<<");
        saveIgniteRdd(botRegistryJavaRDD,ic, botRegistryCacheConfiguration);
    }

    @Override
    public List<BotRegistry> getAllRecords(){
        List<List<?>> all = botsCache
                .query(new SqlFieldsQuery("select * from " + BOT_REGISTRY_TABLE))
                .getAll();
        return all.stream().map(objects -> new BotRegistry(objects.get(0).toString(),
                (Long)objects.get(1),(Integer) objects.get(2),(Integer)objects.get(3),
                (Integer)objects.get(4),(Integer)objects.get(5)))
        .collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public void cleanUp() {
        ic.close(true);
    }

    @Override
    public Dataset<BotRegistry> loadFromIgnite() {
        return ic.ic().sqlContext()
                .read()
                .format(FORMAT_IGNITE())
                .option(OPTION_TABLE(),IgniteBotRegistryStrategy.BOT_REGISTRY_TABLE)
                .option(OPTION_CONFIG_FILE(),IgniteEventStrategy.CONFIG_FILE)
                .load().as(Encoders.bean(BotRegistry.class));
    }

    @Override
    public Dataset<Row> loadFromCache() {
        return ic.fromCache(botRegistryCacheConfiguration).sql("select * from " + BOT_REGISTRY_TABLE);
    }

    @Override
    public CacheConfiguration<Long, BotRegistry> getBotRegistryCacheConfiguration() {
        return new CacheConfiguration<>(botRegistryCacheConfiguration);
    }

    private void setExpirePolicy(){
        botRegistryCacheConfiguration.setExpiryPolicyFactory(ModifiedExpiryPolicy
                .factoryOf(new Duration(TimeUnit.SECONDS,TTL)));
    }

}
