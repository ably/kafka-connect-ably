package com.ably.kafka.connect;

import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.config.ConfigValueEvaluator;
import com.ably.kafka.connect.mapping.MessageSinkMapping;
import com.ably.kafka.connect.mapping.MessageSinkMappingImpl;
import com.google.gson.JsonObject;
import io.ably.lib.types.Message;
import io.ably.lib.types.MessageExtras;
import io.ably.lib.util.JsonUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class MessageSinkMappingTest {
    private static final String STATIC_MESSAGE_NAME = "static-message";
    private static final String DYNAMIC_MESSAGE_PATTERN = "message_#{topic}_#{key}";
    private static final Map<String,String> baseConfigMap = Map.of("channel", "channelX", "client.key",
            "test-key", "client.id", "test-id");
    private static final   Map<String,String> configMapWithStaticMessageName = Map.of("channel", "channelX", "client.key",
            "test-key", "client.id", "test-id","message.name", STATIC_MESSAGE_NAME);
    private static final   Map<String,String> configMapWithPatternedMessageName = Map.of("channel", "channelX", "client.key",
            "test-key", "client.id", "test-id","message.name", DYNAMIC_MESSAGE_PATTERN);
    private final ConfigValueEvaluator evaluator = new ConfigValueEvaluator();

    private MessageSinkMapping sinkMapping;


    @Test
    void testGetMessage_messageNameIsNullWhenNotProvided() {
        //given
        sinkMapping = new MessageSinkMappingImpl(new ChannelSinkConnectorConfig(baseConfigMap),evaluator);
        final SinkRecord record = new SinkRecord("not_important", 0, Schema.BYTES_SCHEMA, "key".getBytes(), Schema.BYTES_SCHEMA, "value", 0);

        //when
        final Message message = sinkMapping.getMessage(record);

        //then
        assertNull(message.name);
    }

    @Test
    void testGetMessage_messageNameIsStaticWhenStaticConfigProvided() {
        //given
        sinkMapping = new MessageSinkMappingImpl(new ChannelSinkConnectorConfig(configMapWithStaticMessageName),evaluator);
        final SinkRecord record = new SinkRecord("not_important", 0, Schema.BYTES_SCHEMA, "key".getBytes(), Schema.BYTES_SCHEMA, "value", 0);

        //when
        final Message message = sinkMapping.getMessage(record);

        //then
        assertEquals(STATIC_MESSAGE_NAME,message.name);
    }

    @Test
    void testGetMessage_messageNameIsInterpolatedWhenPatternedConfigProvided() {
        //given
        //"message_#{topic}_#{key}"
        sinkMapping = new MessageSinkMappingImpl(new ChannelSinkConnectorConfig(configMapWithPatternedMessageName),evaluator);
        final SinkRecord record = new SinkRecord("niceTopic", 0, Schema.BYTES_SCHEMA, "niceKey".getBytes(), Schema.BYTES_SCHEMA, "value", 0);

        //when
        final Message message = sinkMapping.getMessage(record);

        //then
        assertEquals("message_niceTopic_niceKey",message.name);
    }


    @Test
    void testGetMessage_messageDataIsTheSameWithRecordValue() {
        //given
        sinkMapping = new MessageSinkMappingImpl(new ChannelSinkConnectorConfig(baseConfigMap),evaluator);
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, "key".getBytes(), Schema.BYTES_SCHEMA, "value", 0);

        //when
        final Object messageData = sinkMapping.getMessage(record).data;

        //then
        assertEquals(record.value(), messageData);
    }


    @Test
    void testGetMessage_messageIdIsSetBasedOnRecordValues() {
        //given
        sinkMapping = new MessageSinkMappingImpl(new ChannelSinkConnectorConfig(baseConfigMap),evaluator);
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, "key".getBytes(), Schema.BYTES_SCHEMA, "value", 0);

        //when
        final String messageId = sinkMapping.getMessage(record).id;

        //then
        assertEquals(messageId, String.format("%d:%d:%d", record.topic().hashCode(), record.kafkaPartition(), record.kafkaOffset()));
    }

    ///Please beware that keyws we are using here are not the same as the ones used here is different than the key we use for interpolation
    @Test
    void testGetMessage_sentAndReceivedExtrasKeysAreTheSame() {
        //given
        sinkMapping = new MessageSinkMappingImpl(new ChannelSinkConnectorConfig(baseConfigMap),evaluator);
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, "key".getBytes(), Schema.BYTES_SCHEMA, "value", 0);

        //when
        final MessageExtras messageExtras = sinkMapping.getMessage(record).extras;
        JsonUtils.JsonUtilsObject extras = JsonUtils.object();
        byte[] key = (byte[]) record.key();
        extras.add("key", Base64.getEncoder().encodeToString(key));

        final JsonObject receivedObject = messageExtras.asJsonObject().get("kafka").getAsJsonObject();

        String receivedKey = receivedObject.get("key").getAsString();
        String sentKey = Base64.getEncoder().encodeToString(key);

        //then
        assertEquals(receivedKey, sentKey);
    }

    @Test
    void testGetMessage_recordHeadersAreReceivedCorrectly() {
        //given
        sinkMapping = new MessageSinkMappingImpl(new ChannelSinkConnectorConfig(baseConfigMap),evaluator);
        final List<Header> headersList = new ArrayList<>();
        final Map<String, String> headersMap = Map.of("key1", "value1", "key2", "value2");
        for (Map.Entry<String, String> entry : headersMap.entrySet()) {
            headersList.add(new Header() {
                @Override
                public String key() {
                    return entry.getKey();
                }

                @Override
                public Schema schema() {
                    return null;
                }

                @Override
                public Object value() {
                    return entry.getValue();
                }

                @Override
                public Header with(Schema schema, Object value) {
                    return null;
                }

                @Override
                public Header rename(String key) {
                    return null;
                }
            });
        }
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, "key".getBytes(), Schema.BYTES_SCHEMA, "value", 0, 0L, null, headersList);

        //when
        final MessageExtras messageExtras = sinkMapping.getMessage(record).extras;

        //then
        final JsonObject receivedObject = messageExtras.asJsonObject().get("kafka").getAsJsonObject();
        final JsonObject receivedHeaders = receivedObject.get("headers").getAsJsonObject();
        assertEquals(receivedHeaders.get("key1").getAsString(), "value1");
        assertEquals(receivedHeaders.get("key2").getAsString(), "value2");
    }
}
