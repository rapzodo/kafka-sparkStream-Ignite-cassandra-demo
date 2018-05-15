package com.gridu.business;

import com.gridu.model.BotRegistry;
import org.apache.ignite.cache.query.Query;
import org.apache.spark.sql.Dataset;

public interface StopBotBusinessService<T,R> {
    R execute(T arg);
}
