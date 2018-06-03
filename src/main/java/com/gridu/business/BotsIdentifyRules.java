package com.gridu.business;

import com.gridu.model.BotRegistry;
import com.gridu.utils.StopBotUtils;
import org.apache.spark.api.java.function.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BotsIdentifyRules implements FilterFunction<BotRegistry> {
    private static final Logger logger = LoggerFactory.getLogger(BotsIdentifyRules.class);

    private static final long REQUESTS_LIMIT = Long.valueOf(StopBotUtils.getProperty("requests.limit","10000"));
    private static final long VIEWS_CLICKS_DIFF_LIMIT = Long.valueOf(StopBotUtils.getProperty("clicks.views.diff.limit","3"));
    private static final long CATEGORIES_LIMIT = Long.valueOf(StopBotUtils.getProperty("categories.limit","3"));

    public BotsIdentifyRules(){
        logger.info(">>>BOTS IDENTIFYING RULES LIMITS : REQUESTS {}, DIFF {}, CATEGORIES {}<<<",
                REQUESTS_LIMIT, VIEWS_CLICKS_DIFF_LIMIT,CATEGORIES_LIMIT);
    }

    @Override
    public boolean call(BotRegistry botRegistry) {
        return botRegistry.getEvents() > REQUESTS_LIMIT &&
                botRegistry.getCategories() > CATEGORIES_LIMIT &&
                botRegistry.getViewsClicksDiff() > VIEWS_CLICKS_DIFF_LIMIT;
    }
}
