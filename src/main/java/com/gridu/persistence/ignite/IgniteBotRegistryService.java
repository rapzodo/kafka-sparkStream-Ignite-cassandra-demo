package com.gridu.persistence.ignite;

import com.gridu.model.BotRegistry;
import com.gridu.utils.StopBotIgniteUtils;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.ignite.spark.IgniteDataFrameSettings.*;

public class IgniteBotRegistryService implements IgniteService<Long, BotRegistry> {

    private static final Logger logger = LoggerFactory.getLogger(IgniteBotRegistryService.class);

    public static final String BOT_REGISTRY_TABLE = "BOTREGISTRY";
    public static final String BOT_REGISTRY_CACHE = "botRegistryCache";
    private JavaIgniteContext<Long,BotRegistry> ic;
    private CacheConfiguration<Long,BotRegistry> cacheConfiguration;
    private IgniteCache<Long,BotRegistry> botsCache;

    public IgniteBotRegistryService(JavaIgniteContext javaIgniteContext) {
        ic = javaIgniteContext;
        setup();
    }

    @Override
    public void setup() {
        cacheConfiguration = new CacheConfiguration<>(BOT_REGISTRY_CACHE);
        cacheConfiguration.setIndexedTypes(Long.class, BotRegistry.class);
        setExpirePolicy();
        botsCache = ic.ignite().getOrCreateCache(cacheConfiguration);
        StopBotIgniteUtils.getTables().show();
    }

    @Override
    public void persist(Dataset<BotRegistry> datasets) {
        final boolean tableExists = StopBotIgniteUtils.doesTableExists(BOT_REGISTRY_TABLE);

        IgniteService.save(datasets, BOT_REGISTRY_TABLE, IgniteEventService.CONFIG_FILE,
                "ip,url","template=partitioned",
                tableExists ? SaveMode.Append : SaveMode.Ignore);
    }

    @Override
    public List<BotRegistry> getAllRecords(){
        List<List<?>> all = botsCache
                .query(new SqlFieldsQuery("select * from " + BOT_REGISTRY_TABLE))
                .getAll();
        return all.stream().map(objects -> new BotRegistry(objects.get(0).toString(),
                objects.get(1).toString(),
                Long.valueOf(objects.get(2).toString())))
        .collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public void cleanUp() {
        ic.close(true);
    }

    @Override
    public Dataset<BotRegistry> loadFromIgnite() {
        return ic.ic().sqlContext().read().format(FORMAT_IGNITE())
                .option(OPTION_TABLE(),IgniteBotRegistryService.BOT_REGISTRY_TABLE)
                .option(OPTION_CONFIG_FILE(),IgniteEventService.CONFIG_FILE)
                .load().as(Encoders.bean(BotRegistry.class));
    }

    private void setExpirePolicy(){
        cacheConfiguration.setExpiryPolicyFactory(ModifiedExpiryPolicy
                .factoryOf(new Duration(TimeUnit.SECONDS,TTL)));
    }

}
