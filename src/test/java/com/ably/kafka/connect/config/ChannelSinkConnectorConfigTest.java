package com.ably.kafka.connect.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ChannelSinkConnectorConfigTest {

    @Test
    public void testEndpointIsUnsetByDefault() {
        assertNull(configure(Map.of()).clientOptions.endpoint);
    }

    @Test
    public void testEndpointIsUsedWhenSet() {
        final ChannelSinkConnectorConfig config = configure(Map.of(
            ChannelSinkConnectorConfig.CLIENT_ENDPOINT, "nonprod:sandbox",
            ChannelSinkConnectorConfig.CLIENT_REST_HOST, "rest.example.com",
            ChannelSinkConnectorConfig.CLIENT_ENVIRONMENT, "sandbox"
        ));
        assertEquals("nonprod:sandbox", config.clientOptions.endpoint);
    }

    /**
     * Legacy rest host setting must keep working now that ably-java only accepts an endpoint.
     */
    @Test
    public void testRestHostFallsBackToEndpoint() {
        final ChannelSinkConnectorConfig config = configure(Map.of(
            ChannelSinkConnectorConfig.CLIENT_REST_HOST, "rest.example.com",
            ChannelSinkConnectorConfig.CLIENT_ENVIRONMENT, "sandbox"
        ));
        assertEquals("rest.example.com", config.clientOptions.endpoint);
    }

    /**
     * Legacy environment setting must keep working now that ably-java only accepts an endpoint.
     */
    @Test
    public void testEnvironmentFallsBackToEndpoint() {
        final ChannelSinkConnectorConfig config = configure(Map.of(
            ChannelSinkConnectorConfig.CLIENT_ENVIRONMENT, "sandbox"
        ));
        assertEquals("sandbox", config.clientOptions.endpoint);
    }

    private static ChannelSinkConnectorConfig configure(final Map<String, String> settings) {
        final Map<String, String> mutableCopy = new HashMap<>(settings);
        mutableCopy.put("client.key", "unused_key");
        mutableCopy.put("client.id", "unused_id");
        mutableCopy.put("channel", "unused_channel");
        return new ChannelSinkConnectorConfig(mutableCopy);
    }
}
