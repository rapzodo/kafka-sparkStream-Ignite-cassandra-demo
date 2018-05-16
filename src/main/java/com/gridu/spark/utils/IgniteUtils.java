package com.gridu.spark.utils;

import com.gridu.persistence.ignite.IgniteEventDao;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.catalog.Catalog;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.ignite.IgniteSparkSession;

public class IgniteUtils {

    public static Catalog getCatalog(){
           return getIgniteSparkSession().catalog();
    }

    public static Dataset<Table> getTables(){
        return getCatalog().listTables();
    }

    public static boolean doesTableExists(String tableName){
        return getCatalog().tableExists(tableName);
    }

    public static IgniteSparkSession getIgniteSparkSession() {
        return IgniteSparkSession.builder()
                 .appName("Spark Ignite catalog example")
                 .master("local")
                 .config("spark.executor.instances", "2")
                 //Only additional option to refer to Ignite cluster.
                 .igniteConfig(IgniteEventDao.CONFIG_FILE)
                 .getOrCreate();
    }
}
