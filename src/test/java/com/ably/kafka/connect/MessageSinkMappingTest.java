package com.ably.kafka.connect;

import com.google.gson.JsonObject;
import io.ably.lib.types.Message;
import io.ably.lib.types.MessageExtras;
import io.ably.lib.util.JsonUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MessageSinkMappingTest {
    private MessageSinkMapping sinkMapping = new MessageSinkMappingImpl();

    /**
     * Follwoing tests only test the current state where the name is static, this is going to change when we base message
     * name on record
     */
    @Test
    void testGetSink_messageNameIsAlwaysSink() {
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, "key".getBytes(), Schema.BYTES_SCHEMA, "value", 0);
        final Message message = sinkMapping.getMessage(record);
        assertEquals(message.name, "sink");
    }

    //we always expect byte arrays
    @Test
    void testGetSink_messageConversionFailsWithInvalidKeyType() {
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, "key", Schema.BYTES_SCHEMA, "value", 0);
        assertThrows(ClassCastException.class, () -> {
            sinkMapping.getMessage(record);
        });
    }

    @Test
    void testGetSink_messageDataIsTheSameWithRecordValue() {
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, "key".getBytes(), Schema.BYTES_SCHEMA, "value", 0);
        final Object recordValue = record.value();
        final Object messageData = sinkMapping.getMessage(record).data;
        assertEquals(recordValue, messageData);
    }

    @Test
    void testGetSink_messageId() {
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, "key".getBytes(), Schema.BYTES_SCHEMA, "value", 0);
        final String messageId = sinkMapping.getMessage(record).id;
        assertEquals(messageId, String.format("%d:%d:%d", record.topic().hashCode(), record.kafkaPartition(), record.kafkaOffset()));
    }

    @Test
    void testGetSink_messageExtras_sentAndReceivedKeysAreTheSame() {
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, "key".getBytes(), Schema.BYTES_SCHEMA, "value", 0);
        final MessageExtras messageExtras = sinkMapping.getMessage(record).extras;
        final JsonUtils.JsonUtilsObject kafkaExtras = createKafkaExtras(record);
        final JsonObject receivedObject = messageExtras.asJsonObject().get("kafka").getAsJsonObject();

        String receivedKey = receivedObject.get("key").getAsString();
        String sentKey = kafkaExtras.toJson().get("key").getAsString();
        assertEquals(receivedKey, sentKey);
    }

    @Test
    void testGetSink_messageExtras_recordHeaders() {
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, "key".getBytes(), Schema.BYTES_SCHEMA, "value", 0);
        final MessageExtras messageExtras = sinkMapping.getMessage(record).extras;
        final JsonObject receivedObject = messageExtras.asJsonObject().get("kafka").getAsJsonObject();

        JsonUtils.JsonUtilsObject headers = JsonUtils.object();
        for (Header header : record.headers()) {
            headers.add(header.key(), header.value());
        }
        assertEquals(receivedObject.get("headers").getAsJsonArray(), headers.toJson());
    }

    //a copy of message extras function
    private JsonUtils.JsonUtilsObject createKafkaExtras(SinkRecord record) {
        JsonUtils.JsonUtilsObject extras = JsonUtils.object();

        byte[] key = (byte[]) record.key();
        if (key != null) {
            extras.add("key", Base64.getEncoder().encodeToString(key));
        }

        if (!record.headers().isEmpty()) {
            JsonUtils.JsonUtilsObject headers = JsonUtils.object();
            for (Header header : record.headers()) {
                headers.add(header.key(), header.value());
            }
            extras.add("headers", headers);
        }

        return extras;
    }
}