package com.gridu.persistence.cassandra;

import com.gridu.model.BotRegistry;
import com.gridu.persistence.Repository;
import com.gridu.spark.helpers.SparkArtifactsHelper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;


public class CassandraStrategyTest {

    private JavaSparkContext sparkContext;
    private CassandraService dao;

    @Before
    public void setup(){
        sparkContext = SparkArtifactsHelper
                .createSparkContext("local[*]", "cassandraTest");
        dao = new CassandraService(sparkContext.sc());
    }

    @Test
    public void shouldPersistBotRegistryToCassandra(){
        final JavaRDD<BotRegistry> botRegistryJavaRDD = sparkContext.parallelize(Collections.singletonList(
                new BotRegistry("123", "some.url", 1)));
        dao.persist(botRegistryJavaRDD);
        assertThat(dao.getAllRecords()).hasSize(1);
    }

    @Test
    public void shouldRemoveRecordAfterTTLExpires() throws InterruptedException {
        Thread.sleep((Repository.TTL + 2) * 1000);
        assertThat(dao.getAllRecords().isEmpty()).isTrue();
    }

}