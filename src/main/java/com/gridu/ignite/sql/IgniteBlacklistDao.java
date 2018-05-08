package com.gridu.ignite.sql;

import com.gridu.model.BotRegistry;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalog.Table;

public class IgniteBlacklistDao implements IgniteDao<Long, BotRegistry> {

    public static final String BLACKLIST_TABLE = "BLACKLIST";



    @Override
    public void persist(Dataset<BotRegistry> datasets) {
        IgniteDao.save(datasets, "BLACKLIST",IgniteEventDao.CONFIG_FILE,"ip,url", SaveMode.Append);
    }

    @Override
    public JavaIgniteRDD<Long, BotRegistry> createAnSaveIgniteRdd(JavaRDD<BotRegistry> rdd) {
        return null;
    }

    @Override
    public Dataset<BotRegistry> getEventsDataSetFromJavaRdd(JavaIgniteRDD<Long, BotRegistry> rdd) {
        return null;
    }

    @Override
    public Dataset<Row> aggregateAndCountUrlActionsByIp(JavaIgniteRDD<Long, BotRegistry> rdd) {
        return null;
    }

    @Override
    public Dataset<BotRegistry> identifyBots(Dataset<Row> aggregatedDs, long threshold) {
        return null;
    }

    @Override
    public void closeResource() {

    }
}
