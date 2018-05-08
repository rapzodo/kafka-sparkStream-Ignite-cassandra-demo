package com.gridu.ignite.sql;

import com.gridu.model.BotRegistry;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalog.Table;

public interface IgniteDao<K,T> {

    Dataset<Table> getDataTables();

    void persist(Dataset<T> datasets);

    JavaIgniteRDD<K,T> createAnSaveIgniteRdd(JavaRDD<T> rdd);

    Dataset<T> getEventsDataSetFromJavaRdd(JavaIgniteRDD<K,T> rdd);

    Dataset<Row> aggregateAndCountUrlActionsByIp(JavaIgniteRDD<K, T> rdd);

    Dataset<BotRegistry> identifyBots(Dataset<Row> aggregatedDs, long threshold);

    static void save(Dataset dataset,String table, String configFile,String pKeys,SaveMode saveMode){
        dataset.write()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_TABLE(), table)
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), configFile)
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(),pKeys)
                .mode(saveMode)
                .save();
    }

    void closeResource();
}
