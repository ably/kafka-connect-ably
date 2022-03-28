package com.ably.kafka.connect;

import io.ably.lib.types.ChannelOptions;

interface ChannelConfig {
    String getName();

    ChannelOptions getOptions() throws ChannelSinkConnectorConfig.ConfigException;
}

