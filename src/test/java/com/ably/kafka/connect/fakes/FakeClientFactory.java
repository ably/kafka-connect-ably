package com.ably.kafka.connect.fakes;

import com.ably.kafka.connect.client.AblyClient;
import com.ably.kafka.connect.client.AblyClientFactory;
import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;

import java.util.Map;

public class FakeClientFactory implements AblyClientFactory {
    private final long randomTimeBound;
    private final FakeAblyClient.Listener publishListener;

    public FakeClientFactory(long randomTimeBound, FakeAblyClient.Listener publishListener) {
        this.randomTimeBound = randomTimeBound;
        this.publishListener = publishListener;
    }

    @Override
    public AblyClient create(Map<String, String> settings) throws ChannelSinkConnectorConfig.ConfigException {
        return new FakeAblyClient(randomTimeBound, publishListener);
    }
}
