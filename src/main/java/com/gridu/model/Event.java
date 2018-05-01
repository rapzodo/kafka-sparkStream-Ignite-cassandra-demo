package com.gridu.model;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;

import java.io.Serializable;
import java.util.Objects;

@JsonPropertyOrder({"type","ip","unix_time","url"})
public class Event implements Serializable{

    @JsonProperty
    private String type;
    @JsonProperty
    private java.lang.String ip;
    @JsonProperty("unix_time")
    private long unixTime;
    @JsonProperty
    private java.lang.String url;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public java.lang.String getIp() {
        return ip;
    }

    public void setIp(java.lang.String ip) {
        this.ip = ip;
    }

    public long getUnixTime() {
        return unixTime;
    }

    public void setUnixTime(long unixTime) {
        this.unixTime = unixTime;
    }

    public java.lang.String getUrl() {
        return url;
    }

    public void setUrl(java.lang.String url) {
        this.url = url;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Event)) return false;
        Event event = (Event) o;
        return getUnixTime() == event.getUnixTime() &&
                getType() == event.getType() &&
                Objects.equals(getIp(), event.getIp()) &&
                Objects.equals(getUrl(), event.getUrl());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getType(), getIp(), getUnixTime(), getUrl());
    }
}
