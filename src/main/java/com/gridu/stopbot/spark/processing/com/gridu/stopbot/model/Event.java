package com.gridu.stopbot.spark.processing.com.gridu.stopbot.model;

import com.gridu.stopbot.spark.processing.com.gridu.stopbot.enums.EventType;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

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

    public String toJson(){

        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Event fromJson(String jsonString){
        try {
            return new ObjectMapper().readValue(jsonString,Event.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
