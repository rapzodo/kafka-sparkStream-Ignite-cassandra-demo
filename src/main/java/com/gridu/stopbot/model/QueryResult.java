package com.gridu.stopbot.model;

public class QueryResult {
    private String ip;
    private String url;
    private long numberOfActions;

    public QueryResult(){}

    public QueryResult(String ip, String url, long numberOfClicks) {
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
}
