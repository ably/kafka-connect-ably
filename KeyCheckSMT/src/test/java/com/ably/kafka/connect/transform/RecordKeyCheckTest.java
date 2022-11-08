package com.ably.kafka.connect.transform;


import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

class RecordKeyCheckTest {
    private final RecordKeyCheck<SinkRecord> recordKeyCheck = new RecordKeyCheck<>();

    //config tests

    @Test
    public void empty_config_throws_exception() {
        assertThrows(ConfigException.class, () ->
            recordKeyCheck.configure(Collections.emptyMap()));
    }

    @Test
    public void invalid_channel_name_config_throws_exception() {
        assertThrows(ConfigException.class, () ->
            recordKeyCheck.configure(Collections.singletonMap("channel.name", "invalid")));
    }

    @Test
    public void invalid_message_name_config_throws_exception() {
        assertThrows(ConfigException.class, () ->
            recordKeyCheck.configure(Collections.singletonMap("message.name", "invalid")));
    }

    @Test
    public void valid_message_and_invalid_channel_name_config_throws_exception() {
        assertThrows(ConfigException.class, () ->
            recordKeyCheck.configure(Map.of("channel.name", "invalid_#{topic}", "message.name", "valid_#{key}")));
    }

    @Test
    public void invalid_message_and_valid_channel_name_config_throws_exception() {
        assertThrows(ConfigException.class, () ->
            recordKeyCheck.configure(Map.of("channel.name", "invalid_#{key}", "message.name", "invalid_#{topic}")));
    }


    //non-config tests

    @Test
    public void throwsIllegalArgumentExceptionWhenNullKeyProvidedForChannelName() {
        recordKeyCheck.configure(Collections.singletonMap("channel.name", "valid_#{key}"));
        SinkRecord sinkRecord = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, null, null, null, 0);
        assertThrows(IllegalArgumentException.class, () ->
            recordKeyCheck.apply(sinkRecord));
    }

    @Test
    public void recordStaysTheSameWhenAValidKeyIsProvidedForChannelName() {
        recordKeyCheck.configure(Collections.singletonMap("channel.name", "valid_#{key}"));
        final SinkRecord original = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "key".getBytes(), null, null, 0);
        final SinkRecord transformed = recordKeyCheck.apply(original);
        Assertions.assertEquals(original, transformed);
    }

    @Test
    public void throwsIllegalArgumentExceptionWhenNullKeyProvidedForMessageName() {
        recordKeyCheck.configure(Map.of("channel.name", "valid_#{key}", "message.name", "valid_#{key}"));
        SinkRecord sinkRecord = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, null, null, null, 0);
        assertThrows(IllegalArgumentException.class, () ->
            recordKeyCheck.apply(sinkRecord));
    }

    @Test
    public void recordStaysTheSameWhenAValidKeyIsProvidedForMessageName() {
        recordKeyCheck.configure(Map.of("channel.name", "valid_#{key}", "message.name", "valid_#{key}"));
        final SinkRecord original = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "key".getBytes(), null, null, 0);
        final SinkRecord transformed = recordKeyCheck.apply(original);
        Assertions.assertEquals(original, transformed);
    }

}
