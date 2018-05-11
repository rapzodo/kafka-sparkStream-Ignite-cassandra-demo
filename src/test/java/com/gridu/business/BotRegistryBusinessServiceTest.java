package com.gridu.business;

import com.gridu.ignite.sql.IgniteBotRegistryDao;
import com.gridu.model.BotRegistry;
import com.gridu.spark.helpers.SparkArtifactsHelper;
import com.sun.xml.bind.v2.TODO;
import org.apache.avro.generic.GenericData;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class BotRegistryBusinessServiceTest {

    private BotRegistryBusinessService service;
    private IgniteBotRegistryDao dao;
    private JavaSparkContext sparkContext;
    @Mock
    private JavaIgniteContext igniteContext;

    @Before
    public void setup(){
        sparkContext = SparkArtifactsHelper.createSparkContext("local[*]", "botServiceTest");
        dao = new IgniteBotRegistryDao(igniteContext);
        service = new BotRegistryBusinessService(dao);
    }

    @Test
    public void execute() {
    }

    @Test
    public void shouldIdentifyAndReturnOneBot(){
//        final String expectedIp = "148.67.43.14";
//        long expectedCount = 19;
//        StructType structType = DataTypes.createStructType(new StructField[]{DataTypes.createStructField("ip", DataTypes.StringType,false),
//                DataTypes.createStructField("url", DataTypes.StringType,true),
//                DataTypes.createStructField("count", DataTypes.LongType,false)});
//
//
//
//        Dataset<Row> aggregatedDs = SparkArtifactsHelper.createSparkSession(sparkContext)
//                .createDataFrame(RowFactory.create(expectedIp,"",expectedCount),structType);
//        Dataset<BotRegistry> bots = service.identifyBots(aggregatedDs,18);
//        assertThat(bots.first().getCount()).isEqualTo(19);
//        assertThat(bots.first().getIp()).isEqualTo(expectedIp);
    }

    @Test
    public void identifyBots() {

    }
}