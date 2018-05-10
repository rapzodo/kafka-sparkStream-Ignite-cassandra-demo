package com.gridu.business;

import com.gridu.ignite.sql.IgniteBotRegistryDao;
import com.gridu.model.BotRegistry;
import org.apache.spark.sql.Dataset;

public class BotRegistryBusinessService implements BusinessService<Dataset<BotRegistry>,Void> {
    private IgniteBotRegistryDao dao;

    public BotRegistryBusinessService(IgniteBotRegistryDao dao) {
        this.dao = dao;
    }

    @Override
    public Void execute(Dataset<BotRegistry> bots) {
        dao.persist(bots);
        return null;
    }
}
