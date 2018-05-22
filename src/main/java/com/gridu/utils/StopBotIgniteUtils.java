package com.gridu.utils;

import com.gridu.persistence.ignite.IgniteEventDao;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.catalog.Catalog;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.ignite.IgniteSparkSession;

public class StopBotIgniteUtils {

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

    public static Ignite startIgniteForTests(){
        final IgniteConfiguration cfg = new IgniteConfiguration()
                .setDiscoverySpi(new TcpDiscoverySpi());
        return Ignition.getOrStart(cfg);
    }
}
