package com.gridu.business;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

public class BotsIdentifyRules implements FilterFunction<Row> {


    @Override
    public boolean call(Row row) throws Exception {
        return false;
    }
}
