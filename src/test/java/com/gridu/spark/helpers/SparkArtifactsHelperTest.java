package com.gridu.spark.helpers;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Milliseconds;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SparkArtifactsHelperTest {

    private final String master = "local[*]";
    private final String appName = "test";



    @Test
    public void shouldCreateASparkContextWithAppNameTest(){
        assertThat(SparkArtifactsHelper.createSparkContext(master, appName).appName())
                .isEqualTo(appName);
    }

    @Test
    public void shouldCreateASparkSessionWithAppNametest(){
        assertThat(SparkArtifactsHelper.createSparkSession(master, appName)
                .sparkContext().appName()).isEqualTo(appName);
    }

    @Test
    public void shouldCreateASparkSessionFromJavaSparkContext() {
        assertThat(SparkArtifactsHelper.createSparkSession(SparkArtifactsHelper.createSparkContext(master,appName))
                .sparkContext().appName()).isEqualTo(appName);
    }

    @Test
    public void shouldCreateAJavaStreamingContextWithDuration3000Ms(){
        assertThat(SparkArtifactsHelper.createJavaStreamingContext(master, appName,3)
                    .ssc().checkpointDuration()).isEqualTo(Milliseconds.apply(3000));
    }

}
