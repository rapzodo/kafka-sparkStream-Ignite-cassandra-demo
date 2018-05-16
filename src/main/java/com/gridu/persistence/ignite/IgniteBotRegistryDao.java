package com.gridu.persistence.ignite;

import com.gridu.model.BotRegistry;
import com.gridu.persistence.BaseDao;
import com.gridu.persistence.cassandra.CassandraDao;
import com.gridu.spark.utils.IgniteUtils;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.functions;
import scala.Tuple2;

import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.ignite.spark.IgniteDataFrameSettings.*;

public class IgniteBotRegistryDao implements IgniteDao<Long, BotRegistry> {

    public static final String BOTREGISTRY_TABLE = "BOTREGISTRY";
    public static final String BOTREGISTRY_CACHE = "botRegistryCache";
    private JavaIgniteContext<Long,BotRegistry> ic;
    private CacheConfiguration<Long,BotRegistry> cacheConfiguration;
    private IgniteCache<Long,BotRegistry> botsCache;

    public IgniteBotRegistryDao(JavaIgniteContext javaIgniteContext) {
        ic = javaIgniteContext;
        setup();
    }

    @Override
    public void setup() {
        cacheConfiguration = new CacheConfiguration<>(BOTREGISTRY_CACHE);
        cacheConfiguration.setIndexedTypes(Long.class, BotRegistry.class);
        setExpirePolicy();
        botsCache = ic.ignite().getOrCreateCache(cacheConfiguration);
        IgniteUtils.getTables().show();
    }

    @Override
    public void persist(Dataset<BotRegistry> datasets) {
        final boolean tableExists = IgniteUtils.doesTableExists(BOTREGISTRY_TABLE);

        IgniteDao.save(datasets, BOTREGISTRY_TABLE, IgniteEventDao.CONFIG_FILE,
                "ip,url","template=partitioned",
                tableExists ? SaveMode.Append : SaveMode.Ignore);
    }

    @Override
    public JavaIgniteRDD<Long, BotRegistry> createAnSaveIgniteRdd(JavaRDD<BotRegistry> rdd) {
        JavaIgniteRDD<Long, BotRegistry> igniteRDD = ic.<Long,BotRegistry>fromCache(cacheConfiguration);
        igniteRDD.savePairs(rdd.mapToPair(botRegistry -> new Tuple2<>(IgniteDao.generateIgniteUuid(),botRegistry)));
        return igniteRDD;
    }

    @Override
    public Dataset<BotRegistry> getDataSetFromIgniteJavaRdd(JavaIgniteRDD<Long, BotRegistry> rdd) {
        return rdd.sql("select * from "+ BOTREGISTRY_TABLE).as(Encoders.bean(BotRegistry.class));
    }

    @Override
    public List<BotRegistry> getAllRecords(){
        List<List<?>> all = botsCache
                .query(new SqlFieldsQuery("select * from " + BOTREGISTRY_TABLE))
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
                .option(OPTION_TABLE(),IgniteBotRegistryDao.BOTREGISTRY_TABLE)
                .option(OPTION_CONFIG_FILE(),IgniteEventDao.CONFIG_FILE)
                .load().as(Encoders.bean(BotRegistry.class));
    }

    public void setExpirePolicy(){
        cacheConfiguration.setExpiryPolicyFactory(ModifiedExpiryPolicy
                .factoryOf(new Duration(TimeUnit.SECONDS,TTL)));
    }

}
