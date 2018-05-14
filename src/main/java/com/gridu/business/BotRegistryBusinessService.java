package com.gridu.business;

import com.gridu.persistence.ignite.IgniteBotRegistryDao;
import com.gridu.persistence.ignite.IgniteDao;
import com.gridu.model.BotRegistry;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BotRegistryBusinessService implements StopBotBusinessService<Dataset<Row>,Void> {
    public static final int IP_COL = 0;
    public static final int URL_COL = 1;
    public static final int COUNT_COL = 2;
    private Logger logger = LoggerFactory.getLogger(getClass());
    private IgniteDao dao;
    private static final long ACTIONS_THRESHOLD = 10;

    public BotRegistryBusinessService(IgniteBotRegistryDao dao) {
        this.dao = dao;
    }

    @Override
    public Void execute(Dataset<Row> aggregatedEvents) {
        final Dataset<BotRegistry> bots = identifyBots(aggregatedEvents).cache();
        logger.info("!!!{} BOTS IDENTIFIED!!!",bots.count());
        bots.show();
        dao.persist(bots);
        return null;
    }

    public Dataset<BotRegistry> identifyBots(Dataset<Row> aggregatedDs) {
        return aggregatedDs
                .filter(row -> row.getLong(COUNT_COL) > ACTIONS_THRESHOLD)
                .as(Encoders.bean(BotRegistry.class));
    }

    public Dataset<BotRegistry> removeExpiredBotsFromBlackList(){
        return null;
    }

}
