package com.gridu.model;

import java.io.Serializable;
import java.util.Objects;

public class BotRegistry implements Serializable {
    private String ip;
    private String url;
    private long numberOfActions;

    public BotRegistry(){}

    public BotRegistry(String ip, String url, long numberOfClicks) {
        this.ip = ip;
        this.url = url;
        this.numberOfActions = numberOfClicks;
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

    public long getNumberOfActions() {
        return numberOfActions;
    }

    public void setNumberOfActions(long numberOfActions) {
        this.numberOfActions = numberOfActions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BotRegistry)) return false;
        BotRegistry that = (BotRegistry) o;
        return getNumberOfActions() == that.getNumberOfActions() &&
                Objects.equals(getIp(), that.getIp()) &&
                Objects.equals(getUrl(), that.getUrl());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getIp(), getUrl(), getNumberOfActions());
    }

    @Override
    public String toString() {
        return "BotRegistry{" +
                "ip='" + ip + '\'' +
                ", url='" + url + '\'' +
                ", numberOfActions=" + numberOfActions +
                '}';
    }
}
