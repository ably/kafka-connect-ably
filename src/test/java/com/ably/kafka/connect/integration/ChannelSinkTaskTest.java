package com.ably.kafka.connect.integration;

import com.ably.kafka.connect.AblyHelpers;
import com.ably.kafka.connect.ChannelSinkConnector;
import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import io.ably.lib.realtime.AblyRealtime;
import io.ably.lib.realtime.Channel;
import io.ably.lib.realtime.ConnectionState;
import io.ably.lib.types.AblyException;
import io.ably.lib.types.Message;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.connect.runtime.ConnectorConfig.*;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.junit.jupiter.api.Assertions.*;

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
    private static final int NUM_TASKS = 1;
    public static final long TIMEOUT = 5000L;

    // This needs to be much less than TIMEOUT above, so that buffered data is flushed
    // long before test timeouts
    public static final int TEST_MAX_BUFFERING_DELAY_MS = 100;

    private static final String TOPICS = "topic1,topic2,topic3";
    private static final String DEFAULT_TOPIC = TOPICS.split(",")[0];

    private EmbeddedConnectCluster connectCluster;
    private AblyHelpers.AppSpec appSpec;
    private AblyRealtime ablyClient;

    @BeforeEach
    public void prepTestEnvironment() throws Exception {
        assertDoesNotThrow(() -> appSpec = AblyHelpers.createTestApp(), "Failed to create Ably client");

        connectCluster = new EmbeddedConnectCluster.Builder().build();
        connectCluster.start();
        connectCluster.kafka().createTopic(DEFAULT_TOPIC);

        assertAblyClientIsConnected();
    }

    private void assertAblyClientIsConnected() throws AblyException, InterruptedException {
        ablyClient = AblyHelpers.realtimeClient(appSpec);
        final CountDownLatch latch = new CountDownLatch(1);
        ablyClient.connection.on(connectionStateChange -> {
            if (connectionStateChange.current == ConnectionState.connected) {
                latch.countDown();
            } else if (connectionStateChange.current == ConnectionState.failed) {
                latch.countDown();
                fail("Connection failed");
            }
        });
        latch.await();
    }

    /**
     * Send the connector settings to the embedded cluster and wait for a connector task to start
     *
     * @param settings Connector settings for this test
     * @throws InterruptedException if interrupted while waiting for tasks
     */
    private void configureAndWait(final Map<String, String> settings) throws InterruptedException {
        connectCluster.configureConnector(CONNECTOR_NAME, settings);
        connectCluster.assertions().assertConnectorAndAtLeastNumTasksAreRunning(
            CONNECTOR_NAME, NUM_TASKS, "Connector tasks did not start in time.");
    }

    @AfterEach
    public void clearTestEnvironment() {
        ablyClient.close();
        AblyHelpers.deleteTestApp(appSpec);
        connectCluster.deleteConnector(CONNECTOR_NAME);
        connectCluster.stop();
    }

    @Test
    public void testMessagePublish_correctConfigAtLeastATaskIsRunning() throws Exception {
        final String channelName = "test-channel";

        Map<String, String> settings = createSettings(channelName, null);

        connectCluster.configureConnector(CONNECTOR_NAME, settings);
        connectCluster.assertions().assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, NUM_TASKS, "Connector tasks did not start in time.");
    }

    //channel name tests
    @Test
    public void testMessagePublish_channelExistsWithStaticChannelName() throws InterruptedException {
        final String channelName = "test-channel";

        Map<String, String> settings = createSettings(channelName, null);
        configureAndWait(settings);

        Channel channel = ablyClient.channels.get(channelName);
        AblyHelpers.MessageWaiter messageWaiter = new AblyHelpers.MessageWaiter(channel);

        connectCluster.kafka().produce(DEFAULT_TOPIC, "foo", "bar");

        messageWaiter.waitFor(1, TIMEOUT);
        final List<Message> receivedMessages = messageWaiter.receivedMessages;
        assertReceivedExactAmountOfMessages(receivedMessages, 1);
    }

    @Test
    public void testMessagePublish_ChannelExistsWithTopicPlaceholder() throws InterruptedException {
        final String topicedChannelName = "#{topic}_channel";
        Map<String, String> settings = createSettings(topicedChannelName, null);
        configureAndWait(settings);

        Channel channel = ablyClient.channels.get("topic1_channel");
        AblyHelpers.MessageWaiter messageWaiter = new AblyHelpers.MessageWaiter(channel);

        connectCluster.kafka().produce(DEFAULT_TOPIC, "foo", "bar");

        messageWaiter.waitFor(1, TIMEOUT);
        final List<Message> receivedMessages = messageWaiter.receivedMessages;
        assertReceivedExactAmountOfMessages(receivedMessages, 1);
    }

    @Test
    public void testMessagePublish_ChannelExistsWithTopicAndKeyPlaceholder() throws InterruptedException {
        final String keyName = "key1";
        final String channelName = "#{topic}_#{key}_channel";
        final String messageName = "message1";
        Map<String, String> settings = createSettings(channelName, messageName);
        configureAndWait(settings);

        Channel channel = ablyClient.channels.get("topic1_key1_channel");
        AblyHelpers.MessageWaiter messageWaiter = new AblyHelpers.MessageWaiter(channel);

        connectCluster.kafka().produce(DEFAULT_TOPIC, keyName, "bar");

        messageWaiter.waitFor(1, TIMEOUT);
        final List<Message> receivedMessages = messageWaiter.receivedMessages;
        assertReceivedExactAmountOfMessages(receivedMessages, 1);
    }

    @Test
    public void testMessagePublish_TaskFailedWhenKeyIsNotProvidedButPlaceholderProvided() throws Exception {
        final String channelName = "#{topic}_#{key}_channel";
        final String messageName = "message1";
        Map<String, String> settings = createSettings(channelName, messageName);
        configureAndWait(settings);

        connectCluster.kafka().produce(DEFAULT_TOPIC, null, "bar");
        connectCluster.assertions().assertConnectorIsRunningAndTasksHaveFailed(CONNECTOR_NAME, 1, "Connector tasks did not start in time.");
    }

    @Test
    public void testMessagePublish_MessageReceivedWithTopicPlaceholderMessageName() throws InterruptedException {
        final String channelName = "channel1";
        final String topicedMessageName = "#{topic}_message";
        Map<String, String> settings = createSettings(channelName, topicedMessageName);
        configureAndWait(settings);

        Channel channel = ablyClient.channels.get(channelName);
        AblyHelpers.MessageWaiter messageWaiter = new AblyHelpers.MessageWaiter(channel);

        connectCluster.kafka().produce(DEFAULT_TOPIC, "foo", "bar");

        messageWaiter.waitFor(1, TIMEOUT);
        final List<Message> receivedMessages = messageWaiter.receivedMessages;
        assertReceivedExactAmountOfMessages(receivedMessages, 1);
        assertEquals(receivedMessages.get(0).name, "topic1_message", "Unexpected message name");
    }

    @Test
    public void testMessagePublish_MessageReceivedWithKeyPlaceholderMessageName() throws InterruptedException {
        final String keyName = "key1";
        final String channelName = "channel1";
        final String topicedMessageName = "#{key}_message_d";
        Map<String, String> settings = createSettings(channelName, topicedMessageName);
        configureAndWait(settings);

        Channel channel = ablyClient.channels.get(channelName);
        AblyHelpers.MessageWaiter messageWaiter = new AblyHelpers.MessageWaiter(channel);

        connectCluster.kafka().produce(DEFAULT_TOPIC, keyName, "bar");

        messageWaiter.waitFor(1, TIMEOUT);
        final List<Message> receivedMessages = messageWaiter.receivedMessages;
        assertReceivedExactAmountOfMessages(receivedMessages, 1);
        assertEquals(receivedMessages.get(0).name, "key1_message_d", "Unexpected message name");
    }
    @Test
    public void testMessagePublish_MessageSkippedWithKeyPlaceholderMessageNameWhenNoKeyProvided() throws InterruptedException {
        //given
        final String keyName = "key1";
        final String channelName = "channel1";
        final String messageNamePattern = "#{key}_message_a";
        Map<String, String> settings = createSettings(channelName, messageNamePattern);
        settings.put(ChannelSinkConnectorConfig.SKIP_ON_KEY_ABSENCE, String.valueOf(true));
        configureAndWait(settings);

        Channel channel = ablyClient.channels.get(channelName);
        AblyHelpers.MessageWaiter messageWaiter = new AblyHelpers.MessageWaiter(channel);

         //when
        //half skip half publish
        for (int i = 0; i < 10; i++) {
            if (i % 2 == 0) {
                connectCluster.kafka().produce(DEFAULT_TOPIC, keyName, "bar");
            } else {
                connectCluster.kafka().produce(DEFAULT_TOPIC, null, "bar");
            }
        }

        //then
        messageWaiter.waitFor(10, TIMEOUT);
        final List<Message> receivedMessages = messageWaiter.receivedMessages;
        assertReceivedExactAmountOfMessages(receivedMessages, 5);
        assertEquals(receivedMessages.get(0).name, "key1_message_a", "Unexpected message name");
    }

    @Test
    public void testMessagePublish_MessageSkippedWithKeyPlaceholderMessageWhenKeyProvidedForAllMessages() throws InterruptedException {
        //given
        final String keyName = "key1";
        final String channelName = "channel1";
        final String messageNamePattern = "#{key}_message_b";
        Map<String, String> settings = createSettings(channelName, messageNamePattern);
        settings.put(ChannelSinkConnectorConfig.SKIP_ON_KEY_ABSENCE, String.valueOf(true));
        configureAndWait(settings);

        Channel channel = ablyClient.channels.get(channelName);
        AblyHelpers.MessageWaiter messageWaiter = new AblyHelpers.MessageWaiter(channel);

        //when
        //all publish
        for (int i = 0; i < 10; i++) {
            connectCluster.kafka().produce(DEFAULT_TOPIC, keyName, "bar");
        }

        //then
        messageWaiter.waitFor(10, TIMEOUT);
        final List<Message> receivedMessages = messageWaiter.receivedMessages;
        assertReceivedExactAmountOfMessages(receivedMessages, 10);
        assertEquals(receivedMessages.get(0).name, "key1_message_b", "Unexpected message name");
    }

    @Test
    public void testMessagePublish_MessageSkippedWithKeyPlaceholderChannelNameWhenNoKeyProvided() throws InterruptedException {
        //given
        final String keyName = "key1";
        final String channelName = "channel1_#{key}";
        final String messageName = "my_message";
        Map<String, String> settings = createSettings(channelName, messageName);
        settings.put(ChannelSinkConnectorConfig.SKIP_ON_KEY_ABSENCE, String.valueOf(true));
        configureAndWait(settings);

        Channel channel = ablyClient.channels.get("channel1_key1");
        AblyHelpers.MessageWaiter messageWaiter = new AblyHelpers.MessageWaiter(channel);

        //when
        //half skip half publish
        for (int i = 0; i < 10; i++) {
            if (i % 2 == 0) {
                connectCluster.kafka().produce(DEFAULT_TOPIC, keyName, "bar");
            } else {
                connectCluster.kafka().produce(DEFAULT_TOPIC, null, "bar");
            }
        }

        //then
        messageWaiter.waitFor(10, TIMEOUT);
        final List<Message> receivedMessages = messageWaiter.receivedMessages;
        assertReceivedExactAmountOfMessages(receivedMessages, 5);
    }

    @Test
    public void testMessagePublish_MessageSkippedWithKeyPlaceholderChannelWhenKeyProvidedForAllMessages() throws InterruptedException {
        //given
        final String keyName = "key1";
        final String channelNamePattern = "channel_#{key}";
        final String messageName = "myMessage";
        Map<String, String> settings = createSettings(channelNamePattern, messageName);
        settings.put(ChannelSinkConnectorConfig.SKIP_ON_KEY_ABSENCE, String.valueOf(true));
        configureAndWait(settings);

        Channel channel = ablyClient.channels.get("channel_key1");
        AblyHelpers.MessageWaiter messageWaiter = new AblyHelpers.MessageWaiter(channel);

        //when
        //all publish
        for (int i = 0; i < 10; i++) {
            connectCluster.kafka().produce(DEFAULT_TOPIC, keyName, "bar");
        }

        //then
        messageWaiter.waitFor(10, TIMEOUT);
        final List<Message> receivedMessages = messageWaiter.receivedMessages;
        assertReceivedExactAmountOfMessages(receivedMessages, 10);
    }

    @Test
    public void testMessagePublish_MessageReceivedWithTopicAndKeyPlaceholderMessageName() throws InterruptedException {
        final String keyName = "key1";
        final String channelName = "channel1";
        final String messageNamePattern = "#{topic}_#{key}_message_c";
        Map<String, String> settings = createSettings(channelName, messageNamePattern);
        configureAndWait(settings);

        Channel channel = ablyClient.channels.get(channelName);
        AblyHelpers.MessageWaiter messageWaiter = new AblyHelpers.MessageWaiter(channel);

        connectCluster.kafka().produce(DEFAULT_TOPIC, keyName, "bar");

        messageWaiter.waitFor(1, TIMEOUT);
        final List<Message> receivedMessages = messageWaiter.receivedMessages;
        assertReceivedExactAmountOfMessages(receivedMessages, 1);
        assertEquals(receivedMessages.get(0).name, "topic1_key1_message_c", "Unexpected message name");
    }

    private void assertReceivedExactAmountOfMessages(final List<Message> receivedMessages, int expectedMessageCount) {
        assertEquals(expectedMessageCount, receivedMessages.size(), "Unexpected message count");
    }

    private Map<String, String> createSettings(@Nonnull String channelNamePattern, String messageNamePattern) {
        Map<String, String> settings = new HashMap<>();
        settings.put(CONNECTOR_CLASS_CONFIG, SINK_CONNECTOR_CLASS_NAME);
        settings.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        settings.put(TOPICS_CONFIG, TOPICS);
        settings.put(KEY_CONVERTER_CLASS_CONFIG, ByteArrayConverter.class.getName());
        settings.put(VALUE_CONVERTER_CLASS_CONFIG, ByteArrayConverter.class.getName());
        settings.put(ChannelSinkConnectorConfig.CHANNEL_CONFIG, channelNamePattern);
        settings.put(ChannelSinkConnectorConfig.CLIENT_KEY, appSpec.key());
        settings.put(ChannelSinkConnectorConfig.CLIENT_ID, "kafka-connect-ably-test");
        settings.put(ChannelSinkConnectorConfig.CLIENT_ENVIRONMENT, AblyHelpers.TEST_ENVIRONMENT);
        settings.put(ChannelSinkConnectorConfig.BATCH_EXECUTION_MAX_BUFFER_DELAY_MS, Integer.toString(TEST_MAX_BUFFERING_DELAY_MS));
        if (messageNamePattern != null) {
            settings.put(ChannelSinkConnectorConfig.MESSAGE_CONFIG, messageNamePattern);
        }
        return settings;
    }
}
