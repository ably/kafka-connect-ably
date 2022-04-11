package com.ably.kafka.connect.mapping;

import com.ably.kafka.connect.utils.KafkaExtrasExtractor;
import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.config.ConfigValueEvaluator;
import io.ably.lib.types.Message;
import io.ably.lib.types.MessageExtras;
import io.ably.lib.util.JsonUtils;
import org.apache.kafka.connect.sink.SinkRecord;

import javax.annotation.Nonnull;

import static com.ably.kafka.connect.config.ChannelSinkConnectorConfig.MESSAGE_CONFIG;

public interface MessageSinkMapping {
    Message getMessage(SinkRecord record);
}

