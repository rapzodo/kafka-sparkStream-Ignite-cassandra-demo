import com.gridu.converters.JsonConverter;
import com.gridu.model.Event;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JsonConverterTest {

    @Test
    public void testEventToJsonParsing() {
        String expectedJson = aJsonString().replace(" ","");
        assertThat(JsonConverter.toJson(anEvent())).isEqualTo(expectedJson);
    }

    @Test
    public void testFromJson() {
        assertThat(JsonConverter.fromJson(aJsonString())).isEqualTo(anEvent());
    }

    @Test
    public void shouldReturnNullInCaseOfInvalidJsonString() {
        String jsonString = "{\"type\": \"click\", \"ip\": \"140.94.141.193\", ";
        assertThat(JsonConverter.fromJson(jsonString)).isNull();
    }

    private Event anEvent() {
        Event expected = new Event();
        expected.setIp("71.113.135.145");
        expected.setType("click");
        expected.setUnixTime(1502746071688L);
        expected.setUrl("http://33-zabavi.ru/index.html");
        return expected;
    }

    private String aJsonString() {
        return "{\"type\": \"click\", \"ip\": \"71.113.135.145\", \"unix_time\": 1502746071688, \"url\": \"http://33-zabavi.ru/index.html\"}";
    }

}
