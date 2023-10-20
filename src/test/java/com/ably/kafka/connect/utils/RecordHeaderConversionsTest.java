package com.ably.kafka.connect.utils;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.ably.lib.types.MessageExtras;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jose4j.base64url.Base64;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;


class RecordHeaderConversionsTest {

    @Test
    public void testNoKeyNoHeaderReturnsNull(){
        final SinkRecord record = new SinkRecord("topic", 0, null, null, null, null, 0);
        assertNull(RecordHeaderConversions.toMessageExtras(record));
    }

    @Test
    public void testKeyAddsKeyOnKafkaHeader(){
        //given
        final byte[] keyBytes = "my_key".getBytes();
        final SinkRecord record = new SinkRecord("topic", 0, Schema.BYTES_SCHEMA, keyBytes, null, null, 0);
        final String expectedKey = Base64.encode(keyBytes);

        //when
        final MessageExtras messageExtras = RecordHeaderConversions.toMessageExtras(record);

        //then
        assertNotNull(messageExtras);
        final JsonObject jsonObject = messageExtras.asJsonObject();
        final JsonElement kafkaElement = jsonObject.get("kafka");
        assertNotNull(kafkaElement);
        final JsonElement keyElement = kafkaElement.getAsJsonObject().get("key");
        assertNotNull(keyElement);
        assertEquals(expectedKey, keyElement.getAsString());
    }

    @Test
    public void testStringKeyAddsKeyOnKafkaHeader(){
        //given
        final String myKey = "my_key";
        final SinkRecord record = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, myKey, null, null, 0);

        //when
        final MessageExtras messageExtras = RecordHeaderConversions.toMessageExtras(record);

        //then
        assertNotNull(messageExtras);
        final JsonObject jsonObject = messageExtras.asJsonObject();
        final JsonElement kafkaElement = jsonObject.get("kafka");
        assertNotNull(kafkaElement);
        final JsonElement keyElement = kafkaElement.getAsJsonObject().get("key");
        assertNotNull(keyElement);
        assertEquals(myKey, keyElement.getAsString());
    }

    @Test
    public void testPushAddsPushExtrasWithKey() throws IOException {
        //given
        final byte[] keyBytes = "my_key".getBytes();
        final String expectedKey = Base64.encode(keyBytes);

        //given
        final URL url = RecordHeaderConversionsTest.class.getResource("/example_push_payload.json");
        final String pushHeaderValue =  IOUtils.toString(url, StandardCharsets.UTF_8);
        final JsonElement expected = JsonParser.parseString(pushHeaderValue);

        final Map<String, String> headersMap = Map.of("com.ably.extras.push", pushHeaderValue);
        final List<Header> headersList = MockedHeader.fromMap(headersMap);
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, keyBytes, Schema.BYTES_SCHEMA, "value", 0, 0L, null, headersList);

        //when
        final MessageExtras messageExtras = RecordHeaderConversions.toMessageExtras(record);

        //then
        assertNotNull(messageExtras);
        final JsonObject jsonObject = messageExtras.asJsonObject();
        final JsonElement kafkaElement = jsonObject.get("kafka");
        assertNotNull(kafkaElement);
        final JsonElement keyElement = kafkaElement.getAsJsonObject().get("key");
        assertNotNull(keyElement);
        assertEquals(expectedKey, keyElement.getAsString());

        final JsonElement pushElement = jsonObject.get("push");
        assertEquals(expected, pushElement);
    }

    @Test
    public void testPushAddsPushExtrasWithoutKafkaExtras() throws IOException {
        //given
        final URL url = RecordHeaderConversionsTest.class.getResource("/example_push_payload.json");
        final String pushHeaderValue =  IOUtils.toString(url, StandardCharsets.UTF_8);
        final JsonElement expected = JsonParser.parseString(pushHeaderValue);

        final Map<String, String> headersMap = Map.of("com.ably.extras.push", pushHeaderValue);
        final List<Header> headersList = MockedHeader.fromMap(headersMap);
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, null, Schema.BYTES_SCHEMA, "value", 0, 0L, null, headersList);

        //when
        final MessageExtras messageExtras = RecordHeaderConversions.toMessageExtras(record);

        //then
        assertNotNull(messageExtras);
        final JsonObject jsonObject = messageExtras.asJsonObject();
        final JsonElement kafkaElement = jsonObject.get("kafka");
        assertNull(kafkaElement);

        final JsonElement pushElement = jsonObject.get("push");
        assertEquals(expected, pushElement);
    }

    /**
     * Make sure the rest of push payload is included
     * */
    @Test
    public void testPushAddsPushExtrasWithIcon() throws IOException {
        //given
        final URL url = RecordHeaderConversionsTest.class.getResource("/example_push_payload_with_icon.json");
        final String pushHeaderValue =  IOUtils.toString(url, StandardCharsets.UTF_8);
        final JsonElement expected = JsonParser.parseString(pushHeaderValue);

        final Map<String, String> headersMap = Map.of("com.ably.extras.push", pushHeaderValue);
        final List<Header> headersList = MockedHeader.fromMap(headersMap);

        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, null, Schema.BYTES_SCHEMA, "value", 0, 0L, null, headersList);

        //when
        final MessageExtras messageExtras = RecordHeaderConversions.toMessageExtras(record);

        //then
        assertNotNull(messageExtras);
        final JsonObject jsonObject = messageExtras.asJsonObject();
        final JsonElement kafkaElement = jsonObject.get("kafka");
        assertNull(kafkaElement);

        final JsonElement pushElement = jsonObject.get("push");
        assertEquals(expected, pushElement);
    }

    @Test
    public void testPushAddsPushExtrasWithJsonMap() throws IOException {
        //given
        final URL url = RecordHeaderConversionsTest.class.getResource("/example_push_payload.json");
        final String pushHeaderValue =  IOUtils.toString(url, StandardCharsets.UTF_8);
        final JsonElement expected = JsonParser.parseString(pushHeaderValue);
        final Map pushJsonMap = new Gson().fromJson(pushHeaderValue, Map.class);
        final Map<String, Map> headersMap = Map.of("com.ably.extras.push", pushJsonMap);
        final List<Header> headersList = MockedHeader.fromMap(headersMap);
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, null, Schema.BYTES_SCHEMA, "value", 0, 0L, null, headersList);

        //when
        final MessageExtras messageExtras = RecordHeaderConversions.toMessageExtras(record);

        //then
        assertNotNull(messageExtras);
        final JsonObject jsonObject = messageExtras.asJsonObject();
        final JsonElement kafkaElement = jsonObject.get("kafka");
        assertNull(kafkaElement);

        final JsonElement pushElement = jsonObject.get("push");
        assertEquals(expected, pushElement);
    }
    @Test
    public void testPushAddsPushExtrasWithStringSchema() throws IOException {
       testWithSchema(Schema.STRING_SCHEMA);
    }

    @Test
    public void testPushAddsPushExtrasWithBytesSchema() throws IOException {
        testWithSchema(Schema.BYTES_SCHEMA);
    }

    private void testWithSchema(Schema schema) throws IOException {
        final URL url = RecordHeaderConversionsTest.class.getResource("/example_push_payload.json");
        final String pushHeaderValue =  IOUtils.toString(url, StandardCharsets.UTF_8);
        final JsonElement expected = JsonParser.parseString(pushHeaderValue);

        final Map<String, String> headersMap = Map.of("com.ably.extras.push", pushHeaderValue);
        final List<Header> headersList = MockedHeader.fromMap(headersMap);
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, null, Schema.BYTES_SCHEMA, "value", 0, 0L, null, headersList);

        //when
        final MessageExtras messageExtras = RecordHeaderConversions.toMessageExtras(record);

        //then
        assertNotNull(messageExtras);
        final JsonObject jsonObject = messageExtras.asJsonObject();
        final JsonElement kafkaElement = jsonObject.get("kafka");
        assertNull(kafkaElement);

        final JsonElement pushElement = jsonObject.get("push");
        assertEquals(expected, pushElement);
    }

    @Test
    public void testPushNoPushExtrasWithInvalidPayload_without_notification() throws IOException {
        //given
        final URL url = RecordHeaderConversionsTest.class.getResource("/invalid_push_payload_with_no_notification.json");
        final String pushHeaderValue =  IOUtils.toString(url, StandardCharsets.UTF_8);
        final Map<String, String> headersMap = Map.of("com.ably.extras.push", pushHeaderValue);
        final List<Header> headersList = MockedHeader.fromMap(headersMap);
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, null, Schema.BYTES_SCHEMA, "value", 0, 0L, null, headersList);

        //when
        final MessageExtras messageExtras = RecordHeaderConversions.toMessageExtras(record);

        //then
        assertNull(messageExtras);
    }

    @Test
    public void testPushNoPushExtrasWithInvalidPayload_without_title() throws IOException {
        //given
        final URL url = RecordHeaderConversionsTest.class.getResource("/invalid_push_payload_with_no_title.json");
        final String pushHeaderValue =  IOUtils.toString(url, StandardCharsets.UTF_8);
        final Map<String, String> headersMap = Map.of("com.ably.extras.push", pushHeaderValue);
        final List<Header> headersList = MockedHeader.fromMap(headersMap);
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, null, Schema.BYTES_SCHEMA, "value", 0, 0L, null, headersList);

        //when
        final MessageExtras messageExtras = RecordHeaderConversions.toMessageExtras(record);

        //then
        assertNull(messageExtras);
    }

    @Test
    public void testPushNoPushExtrasWithInvalidPayload_without_body() throws IOException {
        //given
        final URL url = RecordHeaderConversionsTest.class.getResource("/invalid_push_payload_with_no_body.json");
        final String pushHeaderValue =  IOUtils.toString(url, StandardCharsets.UTF_8);
        final Map<String, String> headersMap = Map.of("com.ably.extras.push", pushHeaderValue);
        final List<Header> headersList = MockedHeader.fromMap(headersMap);
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, null, Schema.BYTES_SCHEMA, "value", 0, 0L, null, headersList);

        //when
        final MessageExtras messageExtras = RecordHeaderConversions.toMessageExtras(record);

        //then
        assertNull(messageExtras);
    }

    @Test
    void filtersSpecialAblyHeadersFromExtras() throws IOException {
        // given
        final URL url = RecordHeaderConversionsTest.class.getResource("/example_push_payload_with_icon.json");
        final String pushHeaderValue =  IOUtils.toString(url, StandardCharsets.UTF_8);
        final Map<String, String> headersMap = Map.of(
            "com.ably.extras.push", pushHeaderValue,
            "com.ably.encoding", "json",
            "foo", "bar"
        );
        final List<Header> headersList = MockedHeader.fromMap(headersMap);
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, null, Schema.BYTES_SCHEMA, "value", 0, 0L, null, headersList);

        //when
        final MessageExtras messageExtras = RecordHeaderConversions.toMessageExtras(record);

        //then
        assertEquals("{\"foo\":\"bar\"}", messageExtras.asJsonObject().getAsJsonObject().get("headers").toString());
    }

}
