package com.gridu.business;

import com.gridu.ignite.sql.IgniteDao;
import com.gridu.ignite.sql.IgniteEventDao;
import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.spark.sql.functions.*;

public class EventsBusinessService implements BusinessService<JavaRDD<Event>,Dataset<Row>> {

    private IgniteDao dao;
    private Logger logger = LoggerFactory.getLogger(getClass());

    public EventsBusinessService(IgniteEventDao dao) {
        this.dao = dao;
    }

    @Override
    public Dataset<Row> execute(JavaRDD<Event> eventsRDD) {
        logger.info(">>>> PERSISTING RDD IN IGNITE <<<<");
        final JavaIgniteRDD<Long, Event> igniteRdd = dao.createAnSaveIgniteRdd(eventsRDD);
        final Dataset<Event> eventDataset = dao.getDataSetFromIgniteJavaRdd(igniteRdd).cache();
        eventDataset.show();
        logger.info(">>> AGGREGATING DATASET <<<<");
        final Dataset<Row> aggregatedDataset = dao.aggregateAndCount(eventDataset,
                col("ip"),col("url"))
                .cache();
        aggregatedDataset.show();
        return aggregatedDataset;
    }
}
