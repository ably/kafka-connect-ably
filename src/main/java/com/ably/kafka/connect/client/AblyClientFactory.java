package com.ably.kafka.connect.client;

import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;

import java.util.Map;

public interface AblyClientFactory {
    /**
     * Create a new AblyClient instance.
     *
     * @param settings The connector configuration for the client.
     * @return The client.
     *
     * @throws ChannelSinkConnectorConfig.ConfigException If the client cannot be created.
     */
    AblyClient create(Map<String, String> settings)  throws ChannelSinkConnectorConfig.ConfigException;
}
