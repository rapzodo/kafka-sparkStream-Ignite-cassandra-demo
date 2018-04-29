package com.gridu.stopbot.spark.processors;

public interface EventsProcessor {

    public void process(boolean offsetsAutoCommit);
}
