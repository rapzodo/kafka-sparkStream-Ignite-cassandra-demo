package com.gridu.model;

import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;

import java.io.Serializable;
import java.util.Objects;

@JsonPropertyOrder({"type","ip","unix_time","url"})
public class Event implements Serializable{

    @JsonProperty
    @QuerySqlField(index = true)
    private String type;
    @JsonProperty
    @QuerySqlField(index = true)
    private String ip;
    @JsonProperty("unix_time")
    @QuerySqlField
    private long datetime;
    @JsonProperty("category_id")
    @QuerySqlField(index = true)
    private String category;

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
                getType().equals(event.getType()) &&
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
                "type='" + type + '\'' +
                ", ip='" + ip + '\'' +
                ", datetime=" + datetime +
                ", category='" + category + '\'' +
                '}';
    }
}
