package com.gridu.stopbot.model;

import com.gridu.stopbot.enums.EventType;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import java.util.Objects;

@JsonPropertyOrder({"type","ip","unix_time","url"})
public class Event {

    @JsonProperty
    private EventType type;
    @JsonProperty
    private String ip;
    @JsonProperty("unix_time")
    private long unixTime;
    @JsonProperty
    private String url;

    public EventType getType() {
        return type;
    }

    public void setType(EventType type) {
        this.type = type;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public long getUnixTime() {
        return unixTime;
    }

    public void setUnixTime(long unixTime) {
        this.unixTime = unixTime;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
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
