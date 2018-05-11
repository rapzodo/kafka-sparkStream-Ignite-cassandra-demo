package com.gridu.business;

import com.gridu.ignite.sql.IgniteBotRegistryDao;
import com.gridu.ignite.sql.IgniteDao;
import com.gridu.model.BotRegistry;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.col;

public class BotRegistryBusinessService implements BusinessService<Dataset<Row>,Void> {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private IgniteDao dao;
    private static final long ACTIONS_THRESHOLD = 10;

    public BotRegistryBusinessService(IgniteBotRegistryDao dao) {
        this.dao = dao;
    }

    @Override
    public Void execute(Dataset<Row> aggregatedEvents) {
        final Dataset<BotRegistry> bots = identifyBots(aggregatedEvents, ACTIONS_THRESHOLD).cache();
        logger.info("!!!{} BOTS IDENTIFIED!!!",bots.count());
        bots.show();
        dao.persist(bots);
        return null;
    }

    public Dataset<BotRegistry> identifyBots(Dataset<Row> aggregatedDs, long threshold) {
        return aggregatedDs
                .filter(col("count").gt(threshold))
                .as(Encoders.bean(BotRegistry.class));
    }

}
