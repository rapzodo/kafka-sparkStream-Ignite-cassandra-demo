package com.gridu.persistence.ignite;

import com.gridu.persistence.BaseDao;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalog.Catalog;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.ignite.IgniteSparkSession;

import static org.apache.spark.sql.functions.col;

public interface IgniteDao<K,T> extends BaseDao<T> {

    public static final String CONFIG_FILE = "config/example-ignite.xml";

    default Dataset<Row> aggregateAndCount(Dataset<T> eventDataset, Column... groupedCols){
        if(groupedCols == null || groupedCols.length == 0){
            throw new IllegalArgumentException("at least on column should be provided");
        }
        return eventDataset
                .groupBy(groupedCols)
                .count()
                .orderBy(col("count").desc());
    }

    static void save(Dataset dataset,String table, String configFile,String pKeys,String tableParams,SaveMode saveMode){
        dataset.write()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_TABLE(), table)
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), configFile)
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(),pKeys)
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS(),tableParams)
//                .option(IgniteDataFrameSettings.OPTION_STREAMER_FLUSH_FREQUENCY(),3000)
//                .option(IgniteDataFrameSettings.OPTION_STREAMER_ALLOW_OVERWRITE(),true)
                .mode(saveMode)
                .save();
    }

    static long generateIgniteUuid() {
        return IgniteUuid.randomUuid().localId();
    }

    JavaIgniteRDD<K, T> createAnSaveIgniteRdd(JavaRDD<T> rdd);

    Dataset<T> getDataSetFromIgniteJavaRdd(JavaIgniteRDD<K,T> rdd);

    Dataset<T> loadFromIgnite();

}
