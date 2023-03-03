package com.ably.kafka.connect.fakes;

import com.ably.kafka.connect.client.AblyClient;
import com.ably.kafka.connect.client.AblyClientFactory;
import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;

import java.util.Map;

public class FakeClientFactory implements AblyClientFactory {
    private final long randomTimeBound;

    public FakeClientFactory(long randomTimeBound) {
        this.randomTimeBound = randomTimeBound;
    }

    @Override
    public AblyClient create(Map<String, String> settings) throws ChannelSinkConnectorConfig.ConfigException {
        return new FakeAblyClient(randomTimeBound);
    }
}
