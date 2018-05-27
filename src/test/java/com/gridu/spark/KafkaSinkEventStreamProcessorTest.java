package com.gridu.spark;

import com.gridu.persistence.cassandra.CassandraService;
import com.gridu.persistence.ignite.IgniteEventStrategy;
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

import static com.gridu.StopBotJob.KAFKA_PROPS;
import static com.gridu.StopBotJob.TOPICS;


public class KafkaSinkEventStreamProcessorTest{
    private KafkaSinkEventStreamProcessor processor;
    private JavaStreamingContext javaStreamingContext;
    private IgniteEventStrategy eventService;
    private CassandraService botService;
    private JavaIgniteContext ic;

    @Before
    @Ignore
    public void setup(){
        StopBotIgniteUtils.startIgniteForTests();
        final JavaSparkContext sc = SparkArtifactsHelper.createLocalSparkContext("processorTest");
        javaStreamingContext = new JavaStreamingContext(sc, Seconds.apply(3));
        ic = new JavaIgniteContext(sc,IgniteConfiguration::new);
        eventService = new IgniteEventStrategy(ic);
        botService = new CassandraService(sc.sc());

        processor = new KafkaSinkEventStreamProcessor(TOPICS,KAFKA_PROPS,
                javaStreamingContext,eventService, botService);
        ic.sc().setLogLevel("ERROR");
    }



    @Test
    @Ignore
    public void testProcessing(){
        processor.process();
        Assertions.assertThat(eventService.loadFromIgnite().first()).isNotNull();
        Assertions.assertThat(botService.getAllRecords().isEmpty()).isFalse();
    }

    @After
    @Ignore
    public void cleanUp(){
        ic.ignite().close();
        ic.close(true);
    }

}
