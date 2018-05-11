package com.gridu.business;

import com.gridu.ignite.sql.IgniteEventDao;
import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventsBusinessService implements BusinessService<JavaRDD<Event>,Dataset<BotRegistry>> {

    private static final long ACTIONS_THRESHOLD = 10;
    private IgniteEventDao dao;
    private Logger logger = LoggerFactory.getLogger(getClass());

    public EventsBusinessService(IgniteEventDao dao) {
        this.dao = dao;
    }

    @Override
    public Dataset<BotRegistry> execute(JavaRDD<Event> eventsRDD) {
        logger.info(">>>> persisting Rdd in Ignite <<<<");
        final JavaIgniteRDD<Long, Event> igniteRdd = dao.createAnSaveIgniteRdd(eventsRDD);
        final Dataset<Event> eventDataset = dao.getDataSetFromIgniteJavaRdd(igniteRdd).cache();
        eventDataset.show();
        logger.info(">>> aggregating Dataset <<<<");
        final Dataset<Row> aggregatedDataset = dao.aggregateAndCountUrlActionsByIp(eventDataset).cache();
        aggregatedDataset.show();
        return dao.identifyBots(aggregatedDataset, ACTIONS_THRESHOLD);
    }
}
