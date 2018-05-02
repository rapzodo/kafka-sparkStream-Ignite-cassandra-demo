import com.gridu.converters.JsonConverter;
import com.gridu.model.Event;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JsonConverterTest {

    @Test
    public void testEventToJsonParsing() {
        String expectedJson = StringUtils.deleteWhitespace(aGoodAndCleanJsonString());
        assertThat(JsonConverter.toJson(anEvent())).isEqualTo(expectedJson);
    }

    @Test
    public void shouldParseGoodAndCleanJsonString() {
        assertThat(JsonConverter.fromJson(aGoodAndCleanJsonString())).isEqualTo(anEvent());
    }

    @Test
    public void shouldCleanseAndParseDoubleQuotedJsonString() {
        assertThat(JsonConverter.fromJson(aDoubleQuotedJsonString())).isEqualTo(anEvent());
    }

    @Test
    public void shouldCleanseAndParseDoubleQuoteStartingJsonString() {
        assertThat(JsonConverter.fromJson(aDoubleQuotesStartingJsonString())).isEqualTo(anEvent());
    }

    @Test
    public void shouldCleanseAndParseDoubleQuoteEndingJsonString() {
        assertThat(JsonConverter.fromJson(aDoubleQuotesEndingJsonString())).isEqualTo(anEvent());
    }

    @Test
    public void shouldReturnNullInCaseOfInvalidJsonString() {
        assertThat(JsonConverter.fromJson(anIncompleteJsonString())).isNull();
    }

    @Test
    public void shouldCleanseAndParseTwiceDoubleQuotedFieldStringString() {
        assertThat(JsonConverter.fromJson(aTwiceDoubleQuoteUrlFieldJsonString())).isEqualTo(anEvent());
    }

    @Test
    public void shouldParseMissingFieldJsonString() {
        Event expectedEvent = anEvent();
        expectedEvent.setUrl(null);
        assertThat(JsonConverter.fromJson(aMissingFieldJsonString())).isEqualTo(expectedEvent);
    }

    private Event anEvent() {
        Event expected = new Event();
        expected.setIp("71.113.135.145");
        expected.setType("click");
        expected.setUnixTime(1502746071688L);
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
