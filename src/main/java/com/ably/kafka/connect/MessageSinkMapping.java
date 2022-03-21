package com.ably.kafka.connect;

import io.ably.lib.types.Message;
import io.ably.lib.types.MessageExtras;
import io.ably.lib.util.JsonUtils;
import org.apache.kafka.connect.sink.SinkRecord;

public interface MessageSinkMapping {
    Message getMessage(SinkRecord record);
}

class MessageSinkMappingImpl implements MessageSinkMapping {
    @Override
    public Message getMessage(SinkRecord record) {

        Message message = new Message("sink", record.value());
        //leave this as it is for now
        message.id = String.format("%d:%d:%d", record.topic().hashCode(), record.kafkaPartition(), record.kafkaOffset());

        JsonUtils.JsonUtilsObject kafkaExtras = KafkaExtrasExtractor.createKafkaExtras(record);
        if (kafkaExtras.toJson().size() > 0) {
            message.extras = new MessageExtras(JsonUtils.object().add("kafka", kafkaExtras).toJson());
        }
        return message;
    }


}
