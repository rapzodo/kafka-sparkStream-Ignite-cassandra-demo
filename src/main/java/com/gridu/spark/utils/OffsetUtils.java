package com.gridu.spark.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class OffsetUtils     {

    public static void commitOffSets(JavaRDD<ConsumerRecord<String,String>> rdd, JavaInputDStream is){
        OffsetRange[] offsetRange = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
        ((CanCommitOffsets) is.inputDStream()).commitAsync(offsetRange);
    }

    public static OffsetRange[] getOffSetRanges(JavaRDD<ConsumerRecord<String,String>> rdd){
        return ((HasOffsetRanges) rdd.rdd()).offsetRanges();
    }

    public static List<OffsetRange> getOffsetRanges(JavaRDD<ConsumerRecord<String,String>> rdd, OffsetRange[] offsetRanges){
        List<OffsetRange> result = new ArrayList<>();
        rdd.foreachPartition(consumerRecords -> {
            result.add(offsetRanges[TaskContext.get().partitionId()]);
        });
        return result;
    }

    public static OffsetRange[] createOffsetRanges(String topic, int partition, long from, long until, int offsetRangesLength){
        OffsetRange[] objects =IntStream.rangeClosed(1, offsetRangesLength)
                .mapToObj(value -> OffsetRange.create(topic, partition, from, until))
                .toArray(OffsetRange[]::new);
        return objects;
    }
}
