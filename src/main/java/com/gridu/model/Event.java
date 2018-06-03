package com.gridu.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;
import java.util.Objects;

@JsonPropertyOrder({"unix_time,category_id,ip,type"})
public class Event implements Serializable{

    @JsonProperty("unix_time")
    @QuerySqlField
    private long datetime;
    @JsonProperty("category_id")
    @QuerySqlField(index = true)
    private String category;
    @QuerySqlField(index = true)
    private String ip;
    @QuerySqlField(index = true)
    private String type;

    public Event(){}

    public Event(String type, String ip, long unixTime, String category) {
        this.type = type;
        this.ip = ip;
        this.datetime = unixTime;
        this.category = category;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(java.lang.String ip) {
        this.ip = ip;
    }

    public long getDatetime() {
        return datetime;
    }

    public void setDatetime(long datetime) {
        this.datetime = datetime;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Event)) return false;
        Event event = (Event) o;
        return getDatetime() == event.getDatetime() &&
                Objects.equals(getType(), event.getType()) &&
                Objects.equals(getIp(), event.getIp()) &&
                Objects.equals(getCategory(), event.getCategory());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getType(), getIp(), getDatetime(), getCategory());
    }

    @Override
    public String toString() {
        return "Event{" +
                "datetime=" + datetime +
                ", category='" + category + '\'' +
                ", ip='" + ip + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
