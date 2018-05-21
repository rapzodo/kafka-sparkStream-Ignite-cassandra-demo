package com.gridu.business;

import com.gridu.model.BotRegistry;
import com.gridu.persistence.ignite.IgniteBotRegistryDao;
import com.gridu.spark.helpers.SparkArtifactsHelper;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.*;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class BotRegistryBusinessServiceTest {

    private BotRegistryBusinessService service;
    @Mock
    private IgniteBotRegistryDao dao;
    private JavaSparkContext sparkContext;

    @Before
    public void setup(){
        sparkContext = SparkArtifactsHelper.createSparkContext("local[*]", "botServiceTest");
        dao = mock(IgniteBotRegistryDao.class);
        service = new BotRegistryBusinessService(dao);
    }

    @Test
    public void execute() {
        service.execute(aRowDataSet(1));
        final BotRegistryBusinessService spy = spy(service);
        verify(spy,atMost(1)).identifyBots(any(Dataset.class));
        verify(dao,atMost(1)).persist(any(Dataset.class));

    }

    @Test
    public void shouldIdentifyAndReturnOneBot(){
        long expectedCount = 19;

        Dataset<Row> aggregatedDs = aRowDataSet(expectedCount);

        Dataset<BotRegistry> bots = service.identifyBots(aggregatedDs).cache();

        assertThat(bots.count()).isEqualTo(1);
        assertThat(bots.first()).isNotNull();
        assertThat(bots.first().getCount()).isEqualTo(expectedCount);
    }

    @Test
    public void shouldReturnNullWhenCountDoesNotExceedThreshold(){
        assertThat(service.identifyBots(aRowDataSet(1)).count()).isZero();
    }

    private Dataset<Row> aRowDataSet(long count) {
        StructType structType = DataTypes.createStructType(new StructField[]{DataTypes.createStructField("ip",
                DataTypes.StringType,false),
                DataTypes.createStructField("url", DataTypes.StringType,true),
                DataTypes.createStructField("count", DataTypes.LongType,false)});

        final SparkSession sparkSession = SparkArtifactsHelper.createSparkSession(sparkContext);
        final List<Row> rows = Collections.singletonList(RowFactory.create("123", "anyurl", count));

        return sparkSession
                .createDataFrame(rows,structType);
    }
}