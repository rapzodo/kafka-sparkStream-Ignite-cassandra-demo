package com.gridu.business;

import com.gridu.ignite.sql.IgniteEventDao;
import com.gridu.model.BotRegistry;
import com.gridu.model.Event;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class EventsBusinessService implements BusinessService<JavaRDD<Event>,Dataset<BotRegistry>> {

    private static final long ACTIONS_THRESHOLD = 18;
    private IgniteEventDao dao;

    public EventsBusinessService(IgniteEventDao dao) {
        this.dao = dao;
    }

    @Override
    public Dataset<BotRegistry> execute(JavaRDD<Event> eventsRDD) {
        final JavaIgniteRDD<Long, Event> igniteRdd = dao.createAnSaveIgniteRdd(eventsRDD);
        final Dataset<Event> eventDataset = dao.getDataSetFromIgniteJavaRdd(igniteRdd);
        final Dataset<Row> aggregatedDataset = dao.aggregateAndCountUrlActionsByIp(eventDataset);
        return dao.identifyBots(aggregatedDataset, ACTIONS_THRESHOLD);
    }
}
