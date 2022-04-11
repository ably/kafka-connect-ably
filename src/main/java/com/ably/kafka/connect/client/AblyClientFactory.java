package com.ably.kafka.connect.client;

import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;

import java.util.Map;

public interface AblyClientFactory {
    AblyClient create(Map<String, String> settings)  throws ChannelSinkConnectorConfig.ConfigException;
}
