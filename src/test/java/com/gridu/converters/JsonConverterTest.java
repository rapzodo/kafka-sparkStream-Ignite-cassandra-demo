package com.gridu.converters;

import com.gridu.model.Event;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JsonConverterTest {

    @Test
    public void testEventToJsonParsing() {
        String expectedJson = "{\"ip\":\"172.10.0.67\",\"type\":\"view\",\"unix_time\":1527607613,\"category_id\":\"1009\"}";
        assertThat(JsonEventMessageConverter.toJson(anEvent())).isEqualTo(expectedJson);
    }

    @Test
    public void shouldParseGoodAndCleanJsonString() {
        assertThat(JsonEventMessageConverter.fromJson(aGoodAndCleanJsonString())).isEqualTo(anEvent());
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
    public void shouldParseMissingFieldJsonString() {
        Event expectedEvent = new Event(null,"172.10.0.67",1527607613,"1009");
        assertThat(JsonEventMessageConverter.fromJson(aMissingFieldJsonString())).isEqualTo(expectedEvent);
    }

    private Event anEvent() {
        Event expected = new Event();
        expected.setIp("172.10.0.67");
        expected.setType("view");
        expected.setDatetime(1527607613L);
        expected.setCategory("1009");
        return expected;
    }


    private String aGoodAndCleanJsonString() {
        return "{\"unix_time\":\"1527607613\", \"category_id\":1009, \"ip\":\"172.10.0.67\", \"type\":\"view\"}";
    }

    private String aDoubleQuotesStartingJsonString() {
        return "\"{\"unix_time\":\"1527607613\", \"category_id\":1009, \"ip\":\"172.10.0.67\", \"type\":\"view\"}";
    }

    private String aDoubleQuotesEndingJsonString() {
        return "{\"unix_time\":\"1527607613\", \"category_id\":1009, \"ip\":\"172.10.0.67\", \"type\":\"view\"}\"";
    }

    private String anIncompleteJsonString() {
        return "{\"unix_time\":\"1527607613\", \"category_id\":1009, \"ip\":\"172.10.0.67\", ";
    }

    private String aMissingFieldJsonString() {
        return "{\"unix_time\":\"1527607613\", \"category_id\":1009, \"ip\":\"172.10.0.67\"}";
    }


}
