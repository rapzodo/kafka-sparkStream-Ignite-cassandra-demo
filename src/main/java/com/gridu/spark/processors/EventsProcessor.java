package com.gridu.spark.processors;

import java.io.Serializable;

public interface EventsProcessor extends Serializable{

    public void process();
}
