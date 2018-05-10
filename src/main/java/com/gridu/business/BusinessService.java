package com.gridu.business;

import com.gridu.model.BotRegistry;
import org.apache.spark.sql.Dataset;

public interface BusinessService<T,R> {
    R execute(T arg);
}
