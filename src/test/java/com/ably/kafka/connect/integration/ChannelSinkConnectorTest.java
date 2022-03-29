package com.ably.kafka.connect.integration;

import com.ably.kafka.connect.AblyHelpers;
import com.ably.kafka.connect.ChannelSinkConnector;
import com.ably.kafka.connect.ChannelSinkConnectorConfig;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ChannelSinkConnectorTest {

    private static final String CONNECTOR_NAME = "ably-test-connector";
    private static final String SINK_CONNECTOR_CLASS_NAME = ChannelSinkConnector.class.getSimpleName();
    private static final int NUM_WORKERS = 1;
    private static final int NUM_TASKS = 1;
    private static final String TOPICS = "topic1,topic2,topic3";

    private EmbeddedConnectCluster connectCluster;

    @BeforeEach
    public void setup() throws Exception {
        connectCluster = new EmbeddedConnectCluster.Builder().build();
        connectCluster.start();
        connectCluster.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS, "Initial group of workers did not start in time.");
    }

    @AfterEach
    public void close() {
        connectCluster.stop();
    }

    @Test
    public void testConnector_connectorWorksWithValidConfiguration() throws Exception {
        final String channelName = "test-channel";

        Map<String, String> settings = createSettings(channelName, "test-key", "some_client_id", TOPICS);
        connectCluster.configureConnector(CONNECTOR_NAME, settings);
        connectCluster.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS, "Connector tasks did not start in time.");

        connectCluster.deleteConnector(CONNECTOR_NAME);
    }

    @Test
    public void testConnector_connectorWorksWithProxyNoPassword() throws Exception {
        final String channelName = "test-channel";

        final Map<String, String> settings = createSettings(channelName, "test-key", "some_client_id", TOPICS);
        settings.put(ChannelSinkConnectorConfig.CLIENT_PROXY,"true");
        settings.put(ChannelSinkConnectorConfig.CLIENT_PROXY_HOST,"localhost");
        settings.put(ChannelSinkConnectorConfig.CLIENT_PROXY_PORT,"8080");
        connectCluster.configureConnector(CONNECTOR_NAME, settings);
        connectCluster.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS, "Connector tasks did not start in time.");

        connectCluster.deleteConnector(CONNECTOR_NAME);
    }


    @Test
    public void testConnector_connectorFailsWithNoTopicsGiven() {
        final String channelName = "test-channel";

        Map<String, String> settings = createSettings(channelName, "some_fake_key", "some_client_id", null);
        assertThrows(org.apache.kafka.connect.runtime.rest.errors.ConnectRestException.class, () -> connectCluster.configureConnector(CONNECTOR_NAME, settings));
    }



   /* Settings that accepts all required properties as parameters,
   * Use this to emulate error cases
   * */
    private Map<String, String> createSettings(String channel,
                                               String clientKey,
                                               String clientId,
                                               String topics) {
        Map<String, String> settings = new HashMap<>();
        settings.put(CONNECTOR_CLASS_CONFIG, SINK_CONNECTOR_CLASS_NAME);
        settings.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        settings.put(TOPICS_CONFIG, topics);
        settings.put(KEY_CONVERTER_CLASS_CONFIG, ByteArrayConverter.class.getName());
        settings.put(VALUE_CONVERTER_CLASS_CONFIG, ByteArrayConverter.class.getName());
        settings.put(ChannelSinkConnectorConfig.CHANNEL_CONFIG, channel);
        settings.put(ChannelSinkConnectorConfig.CLIENT_KEY, clientKey);
        settings.put(ChannelSinkConnectorConfig.CLIENT_ID, clientId);
        settings.put(ChannelSinkConnectorConfig.CLIENT_ENVIRONMENT, AblyHelpers.TEST_ENVIRONMENT);
        return settings;
    }
}
