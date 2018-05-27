package com.gridu.spark;

import com.gridu.business.BotRegistryBusinessService;
import com.gridu.business.EventsBusinessService;
import com.gridu.persistence.cassandra.CassandraDao;
import com.gridu.persistence.ignite.IgniteEventDao;
import com.gridu.spark.helpers.SparkArtifactsHelper;
import com.gridu.spark.processors.KafkaSinkEventStreamProcessor;
import com.gridu.utils.StopBotIgniteUtils;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static com.gridu.spark.StopBotJob.KAFKA_PROPS;
import static com.gridu.spark.StopBotJob.TOPICS;


public class KafkaSinkEventStreamProcessorTest{
    private KafkaSinkEventStreamProcessor processor;
    private JavaStreamingContext javaStreamingContext;
    private EventsBusinessService eventService;
    private BotRegistryBusinessService botService;
    private JavaIgniteContext ic;
    private IgniteEventDao igniteEventDao;
    private CassandraDao cassandraDao;

    @Before
    @Ignore
    public void setup(){
        StopBotIgniteUtils.startIgniteForTests();
        final JavaSparkContext sc = SparkArtifactsHelper.createLocalSparkContext("processorTest");
        javaStreamingContext = new JavaStreamingContext(sc, Seconds.apply(3));
        ic = new JavaIgniteContext(sc,IgniteConfiguration::new);
        igniteEventDao = new IgniteEventDao(ic);
        eventService = new EventsBusinessService(igniteEventDao);
        cassandraDao = new CassandraDao(sc.sc());
        botService = new BotRegistryBusinessService(cassandraDao);

        processor = new KafkaSinkEventStreamProcessor(TOPICS,KAFKA_PROPS,
                javaStreamingContext,eventService, botService);
        ic.sc().setLogLevel("ERROR");
    }



    @Test
    @Ignore
    public void testProcessing(){
        processor.process();
        Assertions.assertThat(igniteEventDao.loadFromIgnite().first()).isNotNull();
        Assertions.assertThat(cassandraDao.getAllRecords().isEmpty()).isFalse();
    }

    @After
    @Ignore
    public void cleanUp(){
        ic.ignite().close();
        ic.close(true);
    }

}
