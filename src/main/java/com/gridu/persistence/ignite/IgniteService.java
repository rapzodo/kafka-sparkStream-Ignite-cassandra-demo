package com.gridu.persistence.ignite;

import com.gridu.persistence.Repository;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import static org.apache.spark.sql.functions.col;

public interface IgniteService<K, T> extends Repository<T> {

    String CONFIG_FILE = "config/example-ignite.xml";

    default Dataset<Row> selectAggregateAndCount(JavaIgniteRDD<K, T> igniteRDD, String table, Column... groupedCols) {
        if (groupedCols == null || groupedCols.length == 0) {
            throw new IllegalArgumentException("at least on column should be provided");
        }
        return igniteRDD
                .sql("select * from " + table)
                .groupBy(groupedCols)
                .count()
                .orderBy(col("count").desc());
    }

    static void save(Dataset dataset, String table, String configFile, String pKeys, String tableParams, SaveMode saveMode) {
        dataset.write()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_TABLE(), table)
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), configFile)
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(), pKeys)
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS(), tableParams)
//                .option(IgniteDataFrameSettings.OPTION_STREAMER_FLUSH_FREQUENCY(),3000)
//                .option(IgniteDataFrameSettings.OPTION_STREAMER_ALLOW_OVERWRITE(),true)
                .mode(saveMode)
                .save();
    }

    static long generateIgniteUuid() {
        return IgniteUuid.randomUuid().localId();
    }

    Dataset<T> loadFromIgnite();

}
