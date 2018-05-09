package com.gridu.spark.helpers;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkArtifactsHelper {


    public static JavaSparkContext createSparkContext(String master, String appName) {
        SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster(master);
        return JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(sparkConf));
    }

    public static void setLoggerLevel(SparkConf sparkConf, String level){
        SparkContext.getOrCreate(sparkConf).setLogLevel(level);
    }

    public static SparkSession createSparkSession(String master, String appName) {
        return SparkSession.builder().master(master).appName(appName).getOrCreate();
    }

    public static SparkSession createSparkSession(JavaSparkContext javaSparkContext) {
        return SparkSession.builder().sparkContext(javaSparkContext.sc()).getOrCreate();
    }

    public static JavaStreamingContext createJavaStreamingContext(String master, String appName,long seconds) {
        SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster(master);
        return new JavaStreamingContext(JavaSparkContext
                .fromSparkContext(SparkContext.getOrCreate(sparkConf)),Seconds.apply(seconds));
    }
}
