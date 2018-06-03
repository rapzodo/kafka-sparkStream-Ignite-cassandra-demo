package com.gridu.persistence.ignite;

import com.gridu.persistence.PersistenceStrategy;
import com.gridu.utils.StopBotUtils;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import scala.Tuple2;

import static org.apache.spark.sql.functions.col;

public interface IgniteStrategy<K, T> extends PersistenceStrategy<T> {

    String CONFIG_FILE = "config/example-shared-rdd.xml";

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

    static void saveDataset(Dataset dataset, String table, String configFile, String pKeys, String tableParams, SaveMode saveMode) {
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

    default JavaIgniteRDD<K,T> saveIgniteRdd(JavaRDD<T> rdd, JavaIgniteContext ic,
                                             CacheConfiguration<K,T> cacheConfiguration){
        final JavaPairRDD<Long, T> pairRDD = rdd.mapToPair(T -> new Tuple2<>(StopBotUtils.generateIgniteUuidLocalId(), T));
        final JavaIgniteRDD javaIgniteRDD = ic.fromCache(cacheConfiguration);
        javaIgniteRDD.savePairs(pairRDD,true);
        return javaIgniteRDD;
    }

    Dataset<T> loadFromIgnite();

    Dataset<Row> loadFromCache();

    CacheConfiguration<K, T> getBotRegistryCacheConfiguration();
}
