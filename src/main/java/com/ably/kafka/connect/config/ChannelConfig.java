package com.ably.kafka.connect.config;

import io.ably.lib.types.ChannelOptions;

public interface ChannelConfig {
    String getName();

    ChannelOptions getOptions() throws ChannelSinkConnectorConfig.ConfigException;
}

