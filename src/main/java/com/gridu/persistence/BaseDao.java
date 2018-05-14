package com.gridu.persistence;

import org.apache.spark.sql.Dataset;

import java.util.List;

public interface BaseDao<T> {

    void persist(Dataset<T> t);

    List<T> getAllRecords();

}
