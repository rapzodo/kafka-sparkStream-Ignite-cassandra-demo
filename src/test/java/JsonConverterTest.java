import com.gridu.converters.JsonConverter;
import com.gridu.enums.EventType;
import com.gridu.model.Event;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

public class JsonConverterTest {


    @Test
    public void testEventToJsonParsing() {
        String expectedJson = "{\"type\":\"click\",\"ip\":\"123.345.567\"," +
                "\"unix_time\":12345,\"url\":\"http:someUrl\"}";
        assertThat(JsonConverter.toJson(anEvent())).isEqualTo(expectedJson);
    }

    @Test
    public void testFromJson() {
        assertThat(JsonConverter.fromJson(aJsonString())).isEqualTo(anEvent());
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
