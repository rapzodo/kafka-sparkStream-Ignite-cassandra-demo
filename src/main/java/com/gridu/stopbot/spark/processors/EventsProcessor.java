package com.gridu.stopbot.spark.processors;

import java.io.Serializable;

public interface EventsProcessor extends Serializable{

    public void process(boolean offsetsAutoCommit);
}
