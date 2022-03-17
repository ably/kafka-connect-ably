package com.ably.kafka.connect;

import io.ably.lib.types.Message;
import io.ably.lib.types.MessageExtras;
import io.ably.lib.util.JsonUtils;
import org.apache.kafka.connect.sink.SinkRecord;

import javax.annotation.Nonnull;

import static com.ably.kafka.connect.ChannelSinkConnectorConfig.MESSAGE_CONFIG;

public interface MessageSinkMapping {
    Message getMessage(SinkRecord record);
}

class MessageSinkMappingImpl implements MessageSinkMapping {

    private final ChannelSinkConnectorConfig sinkConnectorConfig;
    private final ConfigValueEvaluator configValueEvaluator;

    public MessageSinkMappingImpl(@Nonnull ChannelSinkConnectorConfig config, ConfigValueEvaluator configValueEvaluator) {
        this.sinkConnectorConfig = config;
        this.configValueEvaluator = configValueEvaluator;
    }

    @Override
    public Message getMessage(SinkRecord record) {
        final String configuredName = sinkConnectorConfig.getString(MESSAGE_CONFIG);
        final String messageName = configuredName != null ?
                configValueEvaluator.evaluate(record, configuredName) : null;
        Message message = new Message(messageName, record.value());
        message.id = String.format("%d:%d:%d", record.topic().hashCode(), record.kafkaPartition(), record.kafkaOffset());

        JsonUtils.JsonUtilsObject kafkaExtras = KafkaExtrasExtractor.createKafkaExtras(record);
        if (kafkaExtras.toJson().size() > 0) {
            message.extras = new MessageExtras(JsonUtils.object().add("kafka", kafkaExtras).toJson());
        }
        return message;
    }


}
