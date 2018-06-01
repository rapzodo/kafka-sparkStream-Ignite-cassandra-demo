package com.gridu.utils;

import com.gridu.persistence.ignite.IgniteStrategy;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.catalog.Catalog;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.ignite.IgniteSparkSession;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

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
                .igniteConfig(IgniteStrategy.CONFIG_FILE)
                 .getOrCreate();
    }

    public static Ignite startIgniteWithTcpDiscoverySpi(){
        final IgniteConfiguration cfg = new IgniteConfiguration()
                .setDiscoverySpi(new TcpDiscoverySpi());
        return Ignition.getOrStart(cfg);
    }

    public static long generateIgniteUuidLocalId(){
        return IgniteUuid.randomUuid().localId();
    }

    public static String getProperty(String propertyName){
        final Properties properties = new Properties();
        try(FileInputStream is = new FileInputStream(new File("config/config.properties"))){
            properties.load(is);
            return properties.getProperty(propertyName);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
