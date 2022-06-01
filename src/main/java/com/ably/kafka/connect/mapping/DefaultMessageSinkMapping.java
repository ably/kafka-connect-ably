package com.ably.kafka.connect.mapping;

import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.config.ConfigValueEvaluator;
import com.ably.kafka.connect.utils.KafkaExtrasExtractor;
import com.ably.kafka.connect.utils.StructToJsonConverter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.ably.lib.types.Message;
import io.ably.lib.types.MessageExtras;
import io.ably.lib.util.JsonUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import javax.annotation.Nonnull;

import static com.ably.kafka.connect.config.ChannelSinkConnectorConfig.MESSAGE_CONFIG;

public class DefaultMessageSinkMapping implements MessageSinkMapping {
    private static final Gson gson = new GsonBuilder().serializeNulls().create();

    private final ChannelSinkConnectorConfig sinkConnectorConfig;
    private final ConfigValueEvaluator configValueEvaluator;

    public DefaultMessageSinkMapping(@Nonnull ChannelSinkConnectorConfig config, @Nonnull ConfigValueEvaluator configValueEvaluator) {
        this.sinkConnectorConfig = config;
        this.configValueEvaluator = configValueEvaluator;
    }

    @Override
    public Message getMessage(SinkRecord record) {
        final Message message = messageFromRecord(record);
        message.id = String.format("%d:%d:%d", record.topic().hashCode(), record.kafkaPartition(), record.kafkaOffset());

        JsonUtils.JsonUtilsObject kafkaExtras = KafkaExtrasExtractor.createKafkaExtras(record);
        if (kafkaExtras.toJson().size() > 0) {
            message.extras = new MessageExtras(JsonUtils.object().add("kafka", kafkaExtras).toJson());
        }
        return message;
    }

    private Message messageFromRecord(SinkRecord record) {
        final String messageName = configValueEvaluator.evaluate(record, sinkConnectorConfig.getString(MESSAGE_CONFIG));

        if (record.valueSchema() == null) {
            return new Message(messageName, record.value());
        }

        final Schema valueSchema = record.valueSchema();

        switch (valueSchema.type()) {
            case STRUCT:
                final String jsonString = StructToJsonConverter.toJsonString((Struct) record.value(), gson);
                final Message message = new Message(messageName, jsonString);
                message.encoding = "json";
                return message;
            case BYTES:
            case STRING:
                return new Message(messageName, record.value());
        }
        throw new ConnectException("Unsupported value schema type: " + valueSchema.type());
    }
}
