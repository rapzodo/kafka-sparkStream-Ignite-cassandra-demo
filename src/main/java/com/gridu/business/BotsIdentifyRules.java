package com.gridu.business;

import com.gridu.model.BotRegistry;
import com.gridu.utils.StopBotUtils;
import org.apache.spark.api.java.function.FilterFunction;

public class BotsIdentifyRules implements FilterFunction<BotRegistry> {

    private static final long REQUESTS_LIMIT = Long.valueOf(StopBotUtils.getProperty("requests.limit","10000"));
    private static final long  VIEWS_CLICKS_DIFF= Long.valueOf(StopBotUtils.getProperty("clicks.views.diff.limit","3"));
    private static final long CATEGORIES_LIMIT = Long.valueOf(StopBotUtils.getProperty("categories.limit","3"));

    @Override
    public boolean call(BotRegistry botRegistry) {
        return botRegistry.getEvents() > REQUESTS_LIMIT &&
                botRegistry.getCategories() > CATEGORIES_LIMIT &&
                botRegistry.getViewsClicksDiff() > VIEWS_CLICKS_DIFF;
    }
}
