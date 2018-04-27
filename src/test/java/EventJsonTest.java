import com.gridu.stopbot.spark.processing.com.gridu.stopbot.enums.EventType;
import com.gridu.stopbot.spark.processing.com.gridu.stopbot.model.Event;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class EventJsonTest {


    @Test
    public void testEventToJsonParsing() throws IOException {
        String expectedJson = "{\"type\":\"click\",\"ip\":\"123.345.567\"," +
                "\"unix_time\":12345,\"url\":\"http:someUrl\"}";
        Assert.assertEquals(expectedJson,anEvent().toJson());
    }

    @Test
    public void testFromJson() throws IOException {
        Assert.assertEquals(anEvent(), Event.fromJson(aJsonString()));
    }

    private Event anEvent() {
        Event expected = new Event();
        expected.setIp("123.345.567");
        expected.setType(EventType.click);
        expected.setUnixTime(12345);
        expected.setUrl("http:someUrl");
        return expected;
    }

    private String aJsonString() {
        return "{\"type\":\"click\",\"ip\":\"123.345.567\"," +
                    "\"unix_time\":\"12345\",\"url\":\"http:someUrl\"}";
    }

}
