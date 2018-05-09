package com.gridu.model;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;
import java.util.Objects;

public class BotRegistry implements Serializable {

    @QuerySqlField(index = true)
    private String ip;
    @QuerySqlField(index = true)
    private String url;
    @QuerySqlField(index = true)
    private long count;

    public BotRegistry(){}

    public BotRegistry(String ip, String url, long numberOfClicks) {
        this.ip = ip;
        this.url = url;
        this.count = numberOfClicks;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BotRegistry)) return false;
        BotRegistry that = (BotRegistry) o;
        return getCount() == that.getCount() &&
                Objects.equals(getIp(), that.getIp()) &&
                Objects.equals(getUrl(), that.getUrl());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getIp(), getUrl(), getCount());
    }

    @Override
    public String toString() {
        return "BotRegistry{" +
                "ip='" + ip + '\'' +
                ", url='" + url + '\'' +
                ", count=" + count +
                '}';
    }
}
