package com.gridu.ignite.sql;

import com.gridu.model.BotRegistry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import static org.apache.ignite.spark.IgniteDataFrameSettings.*;

public class IgniteBotRegistryDao implements IgniteDao<Long, BotRegistry> {

    public static final String BOTREGISTRY_TABLE = "BOTREGISTRY";
    public static final String BOTREGISTRY_CACHE = "botRegistryCache";
    private JavaIgniteContext<Long,BotRegistry> ic;
    private CacheConfiguration<Long,BotRegistry> cacheConfiguration;

    public IgniteBotRegistryDao(JavaSparkContext javaSparkContext) {
        cacheConfiguration = new CacheConfiguration<>(BOTREGISTRY_CACHE);
        cacheConfiguration.setIndexedTypes(Long.class, BotRegistry.class);
//        cacheConfiguration.setSqlSchema("PUBLIC");
        ic = new JavaIgniteContext<>(javaSparkContext, IgniteConfiguration::new);
    }

    @Override
    public void persist(Dataset<BotRegistry> datasets) {
        IgniteDao.save(datasets, BOTREGISTRY_TABLE, IgniteEventDao.CONFIG_FILE,"ip,url","template=partitioned",SaveMode.Append);
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
        IgniteCache<Long, BotRegistry> blacklistCache = ic.ignite().getOrCreateCache(cacheConfiguration);

        List<List<?>> all = blacklistCache
                .query(new SqlFieldsQuery("select * from " + BOTREGISTRY_TABLE))
                .getAll();
        return all.stream().map(objects -> new BotRegistry(objects.get(0).toString(),
                objects.get(1).toString(),
                Long.valueOf(objects.get(2).toString())))
        .collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public Dataset<BotRegistry> loadFromIgnite() {
        return ic.ic().sqlContext().read().format(FORMAT_IGNITE())
                .option(OPTION_TABLE(),IgniteBotRegistryDao.BOTREGISTRY_TABLE)
                .option(OPTION_CONFIG_FILE(),IgniteEventDao.CONFIG_FILE)
                .load().as(Encoders.bean(BotRegistry.class));
    }

    @Override
    public void closeResource() {
        ic.close(true);
    }
}
