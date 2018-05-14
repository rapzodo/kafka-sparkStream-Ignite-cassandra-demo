package com.gridu.persistence.cassandra;

import com.gridu.model.BotRegistry;
import com.gridu.spark.helpers.SparkArtifactsHelper;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;


public class CassandraDaoTest {

    private JavaSparkContext sparkContext;
    private CassandraDao dao;

    @Before
    public void setup(){
        sparkContext = SparkArtifactsHelper
                .createSparkContext("local[*]", "cassandraTest");
        dao = new CassandraDao(sparkContext.sc());
    }

    @Test
    public void shouldPersistBotRegistryToCassandra(){
        final SparkSession session = SparkArtifactsHelper.createSparkSession(sparkContext);
        final Dataset<BotRegistry> botRegistryDataset = session
                .createDataset(Collections.singletonList(
                        new BotRegistry("123", "some.url", 1))
                , Encoders.bean(BotRegistry.class));
        dao.persist(botRegistryDataset);
        assertThat(dao.getAllRecords()).hasSize(1);
    }

    @Test
    public void shouldRemoveRecordAfterTTLExpires() throws InterruptedException {
        Thread.sleep((CassandraDao.TTL_SECS + 2) * 1000);
        assertThat(dao.getAllRecords().isEmpty()).isTrue();
    }

}