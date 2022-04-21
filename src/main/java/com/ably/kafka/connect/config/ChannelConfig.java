package com.ably.kafka.connect.config;

import io.ably.lib.types.ChannelOptions;
import org.apache.kafka.connect.sink.SinkRecord;

public interface ChannelConfig {
    String getName();

    ChannelOptions getOptions(SinkRecord record) throws ChannelSinkConnectorConfig.ConfigException;
}

