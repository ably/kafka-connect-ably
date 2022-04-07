package com.ably.kafka.connect.integration;

import com.ably.kafka.connect.AblyHelpers;
import com.ably.kafka.connect.ChannelSinkConnector;
import com.ably.kafka.connect.ChannelSinkConnectorConfig;
import io.ably.lib.realtime.AblyRealtime;
import io.ably.lib.realtime.Channel;
import io.ably.lib.types.Message;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test for {@link com.ably.kafka.connect.ChannelSinkTask}
 * This test class contains tests to specifically test the conditions where the {@link com.ably.kafka.connect.ChannelSinkTask} should run and
 * different behviours we expect from it. EmbeddedConnectCluster is used to start a Kafka Connect cluster  and to
 * do some assertions of our interest.
 * System under test class is implicitly provided by connector configuration settings.
 * See settings.put(CONNECTOR_CLASS_CONFIG, SINK_CONNECTOR_CLASS_NAME);
 */

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ChannelSinkTaskTest {
    private static final String CONNECTOR_NAME = "ably-test-connector";
    private static final String SINK_CONNECTOR_CLASS_NAME = ChannelSinkConnector.class.getSimpleName();
    private static final int NUM_WORKERS = 1;
    private static final int NUM_TASKS = 1;
    public static final long TIMEOUT = 5000L;
    private static final String TOPICS = "topic1,topic2,topic3";
    private static final String DEFAULT_TOPIC = TOPICS.split(",")[0];

    private EmbeddedConnectCluster connectCluster;
    private AblyHelpers.AppSpec appSpec;
    private AblyRealtime ablyClient;

    @BeforeAll
    public void createApp() throws Exception {
        appSpec = AblyHelpers.createTestApp();
    }

    @AfterAll
    public void deleteApp() throws Exception {
        AblyHelpers.deleteTestApp(appSpec);
    }

    @BeforeEach
    public void setup() throws Exception {
        connectCluster = new EmbeddedConnectCluster.Builder().build();
        connectCluster.start();
        connectCluster.assertions().assertAtLeastNumWorkersAreUp(NUM_WORKERS, "Initial group of workers did not start in time.");

        ablyClient = AblyHelpers.realtimeClient(appSpec);
    }

    @AfterEach
    public void close() {
        connectCluster.stop();
        ablyClient.close();
    }

    @Test
    public void testMessagePublish_correctConfigAtLeastATaskIsRunning() throws Exception {
        final String channelName = "test-channel";

        connectCluster.kafka().createTopic(DEFAULT_TOPIC);
        Map<String, String> settings = createSettings(channelName, null, null, null);

        connectCluster.configureConnector(CONNECTOR_NAME, settings);
        connectCluster.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS, "Connector tasks did not start in time.");

        connectCluster.deleteConnector(CONNECTOR_NAME);
    }

    //channel name tests
    @Test
    public void testMessagePublish_channelExistsWithStaticChannelName() {
        final String channelName = "test-channel";

        connectCluster.kafka().createTopic(DEFAULT_TOPIC);
        Map<String, String> settings = createSettings(channelName, null, null, null);
        connectCluster.configureConnector(CONNECTOR_NAME, settings);

        Channel channel = ablyClient.channels.get(channelName);
        AblyHelpers.MessageWaiter messageWaiter = new AblyHelpers.MessageWaiter(channel);

        connectCluster.kafka().produce(DEFAULT_TOPIC, "foo", "bar");

        messageWaiter.waitFor(1, TIMEOUT);
        final List<Message> receivedMessages = messageWaiter.receivedMessages;
        assertReceivedExactAmountOfMessages(receivedMessages, 1);

        connectCluster.deleteConnector(CONNECTOR_NAME);
    }

    @Test
    public void testMessagePublish_ChannelExistsWithTopicPlaceholder() {
        connectCluster.kafka().createTopic(DEFAULT_TOPIC);

        final String topicedChannelName = "#{topic}_channel";
        Map<String, String> settings = createSettings(topicedChannelName, null, null, null);
        connectCluster.configureConnector(CONNECTOR_NAME, settings);

        Channel channel = ablyClient.channels.get("topic1_channel");
        AblyHelpers.MessageWaiter messageWaiter = new AblyHelpers.MessageWaiter(channel);

        connectCluster.kafka().produce(DEFAULT_TOPIC, "foo", "bar");

        messageWaiter.waitFor(1, TIMEOUT);
        final List<Message> receivedMessages = messageWaiter.receivedMessages;
        assertReceivedExactAmountOfMessages(receivedMessages, 1);

        connectCluster.deleteConnector(CONNECTOR_NAME);
    }

    @Test
    public void testMessagePublish_ChannelExistsWithTopicAndKeyPlaceholder() {
        connectCluster.kafka().createTopic(DEFAULT_TOPIC);
        final String keyName = "key1";
        final String channelName = "#{topic}_#{key}_channel";
        final String messageName = "message1";
        Map<String, String> settings = createSettings(channelName, null, null, messageName);
        connectCluster.configureConnector(CONNECTOR_NAME, settings);

        Channel channel = ablyClient.channels.get("topic1_key1_channel");
        AblyHelpers.MessageWaiter messageWaiter = new AblyHelpers.MessageWaiter(channel);

        connectCluster.kafka().produce(DEFAULT_TOPIC, keyName, "bar");

        messageWaiter.waitFor(1, TIMEOUT);
        final List<Message> receivedMessages = messageWaiter.receivedMessages;
        assertReceivedExactAmountOfMessages(receivedMessages, 1);

        connectCluster.deleteConnector(CONNECTOR_NAME);
    }

    @Test
    public void testMessagePublish_TaskFailedWhenKeyIsNotProvidedButPlaceholderProvided() throws Exception {
        connectCluster.kafka().createTopic(DEFAULT_TOPIC);
        final String channelName = "#{topic}_#{key}_channel";
        final String messageName = "message1";
        Map<String, String> settings = createSettings(channelName, null, null, messageName);
        connectCluster.configureConnector(CONNECTOR_NAME, settings);
        connectCluster.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS, "Connector tasks did not start in time.");

        connectCluster.kafka().produce(DEFAULT_TOPIC, null, "bar");
        connectCluster.assertions().assertConnectorIsRunningAndTasksHaveFailed(CONNECTOR_NAME, 1, "Connector tasks did not start in time.");

        connectCluster.deleteConnector(CONNECTOR_NAME);
    }

    @Test
    public void testMessagePublish_MessageReceivedWithTopicPlaceholderMessageName() {
        connectCluster.kafka().createTopic(DEFAULT_TOPIC);
        final String channelName = "channel1";
        final String topicedMessageName = "#{topic}_message";
        Map<String, String> settings = createSettings(channelName, null, null, topicedMessageName);
        connectCluster.configureConnector(CONNECTOR_NAME, settings);

        Channel channel = ablyClient.channels.get(channelName);
        AblyHelpers.MessageWaiter messageWaiter = new AblyHelpers.MessageWaiter(channel);

        connectCluster.kafka().produce(DEFAULT_TOPIC, "foo", "bar");

        messageWaiter.waitFor(1, TIMEOUT);
        final List<Message> receivedMessages = messageWaiter.receivedMessages;
        assertReceivedExactAmountOfMessages(receivedMessages, 1);
        assertEquals(receivedMessages.get(0).name, "topic1_message", "Unexpected message name");

        connectCluster.deleteConnector(CONNECTOR_NAME);
    }

    @Test
    public void testMessagePublish_MessageReceivedWithKeyPlaceholderMessageName() {
        connectCluster.kafka().createTopic(DEFAULT_TOPIC);
        final String keyName = "key1";
        final String channelName = "channel1";
        final String topicedMessageName = "#{key}_message";
        Map<String, String> settings = createSettings(channelName, null, null, topicedMessageName);
        connectCluster.configureConnector(CONNECTOR_NAME, settings);

        Channel channel = ablyClient.channels.get(channelName);
        AblyHelpers.MessageWaiter messageWaiter = new AblyHelpers.MessageWaiter(channel);

        connectCluster.kafka().produce(DEFAULT_TOPIC, keyName, "bar");

        messageWaiter.waitFor(1, TIMEOUT);
        final List<Message> receivedMessages = messageWaiter.receivedMessages;
        assertReceivedExactAmountOfMessages(receivedMessages, 1);
        assertEquals(receivedMessages.get(0).name, "key1_message", "Unexpected message name");

        connectCluster.deleteConnector(CONNECTOR_NAME);
    }

    @Test
    public void testMessagePublish_MessageReceivedWithTopicAndKeyPlaceholderMessageName() {
        connectCluster.kafka().createTopic(DEFAULT_TOPIC);
        final String keyName = "key1";
        final String channelName = "channel1";
        final String topicedMessageName = "#{topic}_#{key}_message";
        Map<String, String> settings = createSettings(channelName, null, null, topicedMessageName);
        connectCluster.configureConnector(CONNECTOR_NAME, settings);

        Channel channel = ablyClient.channels.get(channelName);
        AblyHelpers.MessageWaiter messageWaiter = new AblyHelpers.MessageWaiter(channel);

        connectCluster.kafka().produce(DEFAULT_TOPIC, keyName, "bar");

        messageWaiter.waitFor(1, TIMEOUT);
        final List<Message> receivedMessages = messageWaiter.receivedMessages;
        assertReceivedExactAmountOfMessages(receivedMessages, 1);
        assertEquals(receivedMessages.get(0).name, "topic1_key1_message", "Unexpected message name");

        connectCluster.deleteConnector(CONNECTOR_NAME);
    }

    private void assertReceivedExactAmountOfMessages(final List<Message> receivedMessages, int expectedMessageCount) {
        assertEquals(receivedMessages.size(), expectedMessageCount, "Unexpected message count");
    }

    private Map<String, String> createSettings(@Nonnull String channel, String cipherKey, String channelParams, String messageName) {
        Map<String, String> settings = new HashMap<>();
        settings.put(CONNECTOR_CLASS_CONFIG, SINK_CONNECTOR_CLASS_NAME);
        settings.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        settings.put(TOPICS_CONFIG, TOPICS);
        settings.put(KEY_CONVERTER_CLASS_CONFIG, ByteArrayConverter.class.getName());
        settings.put(VALUE_CONVERTER_CLASS_CONFIG, ByteArrayConverter.class.getName());
        settings.put(ChannelSinkConnectorConfig.CHANNEL_CONFIG, channel);
        settings.put(ChannelSinkConnectorConfig.CLIENT_KEY, appSpec.key());
        settings.put(ChannelSinkConnectorConfig.CLIENT_ID, "kafka-connect-ably-test");
        settings.put(ChannelSinkConnectorConfig.CLIENT_ENVIRONMENT, AblyHelpers.TEST_ENVIRONMENT);
        if (cipherKey != null) {
            settings.put(ChannelSinkConnectorConfig.CLIENT_CHANNEL_CIPHER_KEY, cipherKey);
        }
        if (channelParams != null) {
            settings.put(ChannelSinkConnectorConfig.CLIENT_CHANNEL_PARAMS, channelParams);
        }
        if (messageName != null) {
            settings.put(ChannelSinkConnectorConfig.MESSAGE_CONFIG, messageName);
        }
        return settings;
    }
}
