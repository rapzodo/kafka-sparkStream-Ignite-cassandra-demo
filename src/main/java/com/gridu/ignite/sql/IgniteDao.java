package com.gridu.ignite.sql;

import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.ignite.IgniteSparkSession;

import java.util.List;

public interface IgniteDao<K,T> {

    static Dataset<Table> getDataTables(){
        IgniteSparkSession igniteSession = IgniteSparkSession.builder()
                .appName("Spark Ignite catalog example")
                .master("local")
                .config("spark.executor.instances", "2")
                //Only additional option to refer to Ignite cluster.
                .igniteConfig(IgniteEventDao.CONFIG_FILE)
                .getOrCreate();


// This will print out info about all SQL tables existed in Ignite.
        return igniteSession.catalog().listTables();
    }

    static void save(Dataset dataset,String table, String configFile,String pKeys,String tableParams,SaveMode saveMode){
        dataset.write()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_TABLE(), table)
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), configFile)
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(),pKeys)
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS(),tableParams)
                .mode(saveMode)
                .save();
    }

    static long generateIgniteUuid() {
        return IgniteUuid.randomUuid().localId();
    }

    void persist(Dataset<T> datasets);

    JavaIgniteRDD<K, T> createAnSaveIgniteRdd(JavaRDD<T> rdd);

    Dataset<T> getDataSetFromIgniteJavaRdd(JavaIgniteRDD<K,T> rdd);

    Dataset<Row> aggregateAndCount(Dataset<Event> eventDataset, Column... groupedCols);

    List<T> getAllRecords();

    Dataset<T> loadFromIgnite();

    void closeResource();
}
