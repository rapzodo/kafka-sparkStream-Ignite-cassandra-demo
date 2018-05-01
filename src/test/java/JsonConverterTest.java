import com.gridu.converters.JsonConverter;
import com.gridu.model.Event;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JsonConverterTest {

    @Test
    public void testEventToJsonParsing() {
        java.lang.String expectedJson = "{\"type\":\"click\",\"ip\":\"123.345.567\"," +
                "\"unix_time\":12345,\"url\":\"http:someUrl\"}";
        assertThat(JsonConverter.toJson(anEvent())).isEqualTo(expectedJson);
    }

    @Test
    public void testFromJson() {
        java.lang.String jsonS = "{\"type\": \"click\", \"ip\": \"140.94.141.193\", \"unix_time\": \"1502743967277\", \"url\": \"http://www.school91.kiev.ua/index.php?option=com_content&view=article&id=214:2012-10-10-10-42-28&catid=38:2010-10-09-15-56-39&Itemid=57\"}";
        assertThat(JsonConverter.fromJson(aJsonString())).isEqualTo(anEvent());
    }

    @Test
    public void shouldThrowJsonMappingException(){
        java.lang.String jsonString = "{\"type\": \"click\", \"ip\": \"140.94.141.193\", ";
        JsonConverter.fromJson(jsonString);
    }

    private Event anEvent() {
        Event expected = new Event();
        expected.setIp("123.345.567");
        expected.setType("click");
        expected.setUnixTime(12345);
        expected.setUrl("http:someUrl");
        return expected;
    }

    private java.lang.String aJsonString() {
        return "{\"type\":\"click\",\"ip\":\"123.345.567\"," +
                    "\"unix_time\":\"12345\",\"url\":\"http:someUrl\"}";
    }

}
