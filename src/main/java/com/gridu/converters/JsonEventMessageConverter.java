package com.gridu.converters;

import com.gridu.model.Event;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JsonEventMessageConverter {
    private static Logger LOG = LoggerFactory.getLogger(Event.class);

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static Event fromJson(String jsonString) {
        try {
            jsonString = cleanseJsonString(jsonString);
//            System.out.println(jsonString);
//            if (StringUtils.isNotEmpty(jsonString)) {
//                JsonNode node = objectMapper.readTree(jsonString);
//                Event event = new Event(node.get("type").asText(),
//                        node.get("ip").asText(),
//                        node.get("unix_time").asLong(),
//                        node.get("url").asText());
//                return event;
//            }
            return objectMapper.readValue(jsonString,Event.class);
        } catch (IOException e) {
            LOG.error("Error parsing event JsonMessage :" + jsonString, e);
        }
        return null;
    }

    private static String cleanseJsonString(String jsonString) {
        jsonString = jsonString.replace("\"{", "{");
        jsonString = jsonString.replace("}\"", "}");
        jsonString = jsonString.replace("\\", "");
        jsonString = jsonString.replace("\"\"", "\"");
        return jsonString;
    }

    public static String toJson(Event event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (IOException e) {
            LOG.error("Error parsing Event > Json", e);
        }
        return null;
    }
}
