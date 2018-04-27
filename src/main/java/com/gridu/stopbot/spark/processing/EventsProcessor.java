package com.gridu.stopbot.spark.processing;

import java.util.Map;

public interface EventsProcessor {

    public void process(boolean offsetsAutoCommit);
}
