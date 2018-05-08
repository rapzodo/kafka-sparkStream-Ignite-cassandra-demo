import com.gridu.converters.JsonEventMessageConverter;
import com.gridu.model.Event;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JsonConverterTest {

    @Test
    public void testEventToJsonParsing() {
        String expectedJson = StringUtils.deleteWhitespace(aGoodAndCleanJsonString());
        assertThat(JsonEventMessageConverter.toJson(anEvent())).isEqualTo(expectedJson);
    }

    @Test
    public void shouldParseGoodAndCleanJsonString() {
        assertThat(JsonEventMessageConverter.fromJson(aGoodAndCleanJsonString())).isEqualTo(anEvent());
    }

    @Test
    public void shouldCleanseAndParseDoubleQuotedJsonString() {
        assertThat(JsonEventMessageConverter.fromJson(aDoubleQuotedJsonString())).isEqualTo(anEvent());
    }

    @Test
    public void shouldCleanseAndParseDoubleQuoteStartingJsonString() {
        assertThat(JsonEventMessageConverter.fromJson(aDoubleQuotesStartingJsonString())).isEqualTo(anEvent());
    }

    @Test
    public void shouldCleanseAndParseDoubleQuoteEndingJsonString() {
        assertThat(JsonEventMessageConverter.fromJson(aDoubleQuotesEndingJsonString())).isEqualTo(anEvent());
    }

    @Test
    public void shouldReturnNullInCaseOfInvalidJsonString() {
        assertThat(JsonEventMessageConverter.fromJson(anIncompleteJsonString())).isNull();
    }

    @Test
    public void shouldCleanseAndParseTwiceDoubleQuotedFieldStringString() {
        assertThat(JsonEventMessageConverter.fromJson(aTwiceDoubleQuoteUrlFieldJsonString())).isEqualTo(anEvent());
    }

    @Test
    public void shouldParseMissingFieldJsonString() {
        Event expectedEvent = anEvent();
        expectedEvent.setUrl(null);
        assertThat(JsonEventMessageConverter.fromJson(aMissingFieldJsonString())).isEqualTo(expectedEvent);
    }

    private Event anEvent() {
        Event expected = new Event();
        expected.setIp("71.113.135.145");
        expected.setType("click");
        expected.setDatetime(1502746071688L);
        expected.setUrl("http://33-zabavi.ru/index.html");
        return expected;
    }


    private String aGoodAndCleanJsonString() {
        return "{\"type\": \"click\", \"ip\": \"71.113.135.145\", \"unix_time\": 1502746071688, \"url\": \"http://33-zabavi.ru/index.html\"}";
    }

    private String aDoubleQuotesStartingJsonString() {
        return "\"{\"type\": \"click\", \"ip\": \"71.113.135.145\", \"unix_time\": 1502746071688, \"url\": \"http://33-zabavi.ru/index.html\"}";
    }

    private String aDoubleQuotedJsonString() {
        return "\"{\"type\": \"click\", \"ip\": \"71.113.135.145\", \"unix_time\": 1502746071688, \"url\": \"http://33-zabavi.ru/index.html\"}";
    }

    private String aDoubleQuotesEndingJsonString() {
        return "{\"type\": \"click\", \"ip\": \"71.113.135.145\", \"unix_time\": 1502746071688, \"url\": \"http://33-zabavi.ru/index.html\"}\"";
    }

    private String aTwiceDoubleQuoteUrlFieldJsonString() {
        return "{\"type\": \"click\", \"ip\": \"71.113.135.145\", \"unix_time\": 1502746071688, \"url\": \"http://33-zabavi.ru/index.html\"}\"";
    }

    private String anIncompleteJsonString() {
        return "{\"type\": \"click\", \"ip\": \"140.94.141.193\", ";
    }

    private String aMissingFieldJsonString() {
        return "{\"type\": \"click\", \"ip\": \"140.94.141.193\"}";
    }


}
