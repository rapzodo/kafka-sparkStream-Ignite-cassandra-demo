package com.gridu.persistence;

import java.util.List;

public interface BaseDao<T> {
    long TTL = 3;

    void setup();

    List<T> getAllRecords();

    void cleanUp();

}
