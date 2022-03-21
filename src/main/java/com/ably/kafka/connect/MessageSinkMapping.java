package com.ably.kafka.connect;

import io.ably.lib.types.Message;
import io.ably.lib.types.MessageExtras;
import io.ably.lib.util.JsonUtils;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Base64;

public interface MessageSinkMapping {
    Message getMessage(SinkRecord record);
}

class MessageSinkMappingImpl implements MessageSinkMapping {
    @Override
    public Message getMessage(SinkRecord record) {

        Message message = new Message("sink", record.value());
        message.id = String.format("%d:%d:%d", record.topic().hashCode(), record.kafkaPartition(), record.kafkaOffset());

        JsonUtils.JsonUtilsObject kafkaExtras = createKafkaExtras(record);
        if (kafkaExtras.toJson().size() > 0) {
            message.extras = new MessageExtras(JsonUtils.object().add("kafka", kafkaExtras).toJson());
        }
        return message;
    }

    /**
     * Returns the Kafka extras object to use when converting a Kafka message
     * to an Ably message.
     * <p>
     * If the Kafka message has a key, it is base64 encoded and set as the
     * "key" field in the extras.
     * <p>
     * If the Kafka message has headers, they are set as the "headers" field
     * in the extras.
     *
     * @param record The sink record representing the Kafka message
     * @return The Kafka message extras object
     */
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
