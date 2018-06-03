package com.gridu.persistence.cassandra;

import com.gridu.model.BotRegistry;
import com.gridu.persistence.PersistenceStrategy;
import com.gridu.spark.helpers.SparkArtifactsHelper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;


public class CassandraStrategyTest {

    private JavaSparkContext sparkContext;
    private CassandraStrategy dao;

    @Before
    public void setup(){
        sparkContext = SparkArtifactsHelper
                .createSparkContext("local[*]", "cassandraTest");
        dao = new CassandraStrategy(sparkContext.sc());
    }

    @Test
    public void shouldPersistBotRegistryToCassandra(){
        final JavaRDD<BotRegistry> botRegistryJavaRDD = sparkContext.parallelize(Collections.singletonList(
                new BotRegistry("789.987", 10000 ,3,6,2,5)));
        dao.persist(botRegistryJavaRDD);
        assertThat(dao.getAllRecords()).hasSize(1);
    }

}