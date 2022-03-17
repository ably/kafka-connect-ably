package com.ably.kafka.connect;

import com.google.gson.JsonObject;
import io.ably.lib.types.Message;
import io.ably.lib.types.MessageExtras;
import io.ably.lib.util.JsonUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MessageSinkMappingTest {
    private static final String STATIC_CHANNEL_NAME = "sink-channel";
    private static ChannelSinkConnectorConfig STATIC_CHANNEL_CONFIG = new ChannelSinkConnectorConfig(Map.of("channel", STATIC_CHANNEL_NAME, "client.key", "test-key", "client.id", "test-id"));

    private MessageSinkMapping sinkMapping;

    @BeforeEach
    void setUp() {
        sinkMapping = new MessageSinkMappingImpl(STATIC_CHANNEL_CONFIG, new ConfigValueEvaluator());
    }

    /**
     * Follwoing tests only test the current state where the name is static, this is going to change when we base message
     * name on record
     */
    @Test
    void testGetSink_messageNameIsAlwaysSink() {
        //given
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, "key".getBytes(), Schema.BYTES_SCHEMA, "value", 0);

        //when
        final Message message = sinkMapping.getMessage(record);

        //then
        assertEquals(message.name, "sink");
    }

    //we always expect byte arrays
    @Test
    void testGetSink_messageConversionFailsWithInvalidKeyType() {
        //given
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, "key", Schema.BYTES_SCHEMA, "value", 0);

        //then expect
        assertThrows(ClassCastException.class, () -> {
            //when
            sinkMapping.getMessage(record);
        });
    }

    @Test
    void testGetSink_messageDataIsTheSameWithRecordValue() {
        //given
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, "key".getBytes(), Schema.BYTES_SCHEMA, "value", 0);

        //when
        final Object messageData = sinkMapping.getMessage(record).data;

        //then
        assertEquals(record.value(), messageData);
    }

    @Test
    void testGetSink_messageId() {
        //given
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, "key".getBytes(), Schema.BYTES_SCHEMA, "value", 0);

        //when
        final String messageId = sinkMapping.getMessage(record).id;

        //then
        assertEquals(messageId, String.format("%d:%d:%d", record.topic().hashCode(), record.kafkaPartition(), record.kafkaOffset()));
    }

    @Test
    void testGetSink_messageExtras_sentAndReceivedKeysAreTheSame() {
        //given
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, "key".getBytes(), Schema.BYTES_SCHEMA, "value", 0);

        //when
        final MessageExtras messageExtras = sinkMapping.getMessage(record).extras;

        //then
        JsonUtils.JsonUtilsObject extras = JsonUtils.object();
        byte[] key = (byte[]) record.key();
        extras.add("key", Base64.getEncoder().encodeToString(key));

        final JsonObject receivedObject = messageExtras.asJsonObject().get("kafka").getAsJsonObject();

        String receivedKey = receivedObject.get("key").getAsString();
        String sentKey = Base64.getEncoder().encodeToString(key);
        assertEquals(receivedKey, sentKey);
    }

    @Test
    void testGetSink_messageExtras_recordHeaders() {
        //given
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