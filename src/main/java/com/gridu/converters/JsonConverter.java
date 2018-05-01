package com.gridu.converters;

import com.gridu.model.Event;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JsonConverter {
    private static Logger LOG = LoggerFactory.getLogger(Event.class);

    public static Event fromJson(String jsonString){
        try {
            jsonString = jsonString.replace("\"\"","\"");
            jsonString = jsonString.replace("\\","/");
            return new ObjectMapper().readValue(jsonString,Event.class);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("Error parsing Json > event :" + jsonString, e);
        }
        return null;
    }

    public static String toJson(Event event){
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(event);
        } catch (IOException e) {
            LOG.error("Error parsing Event > Json", e);
        }
        return null;
    }
}
