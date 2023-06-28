package com.ably.kafka.connect;

import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.config.ConfigValueEvaluator;
import com.ably.kafka.connect.mapping.ChannelSinkMapping;
import com.ably.kafka.connect.mapping.DefaultChannelSinkMapping;
import io.ably.lib.realtime.AblyRealtime;
import io.ably.lib.types.AblyException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ChannelSinkMappingTest {
    //dependencies
    private static final String STATIC_CHANNEL_NAME = "sink-channel";
    private static final ChannelSinkConnectorConfig STATIC_CHANNEL_CONFIG = new ChannelSinkConnectorConfig(
        Map.of("channel", STATIC_CHANNEL_NAME,
            "client.key", "test-key",
            "client.id", "test-id")
        );

    private AblyRealtime ablyRealtime;

    /**
     * Construct a channel sink mapping for testing
     * @param config Connector configuration
     * @return ChannelSinkMapping system under test
     */
    private static ChannelSinkMapping getMapping(final ChannelSinkConnectorConfig config) {
        return new DefaultChannelSinkMapping(config, new ConfigValueEvaluator());
    }


    @BeforeEach
    void setUp() throws AblyException {
        ablyRealtime = new AblyRealtime(STATIC_CHANNEL_CONFIG.clientOptions);
    }

    @AfterEach
    void tearDown() {
        ablyRealtime.close();
    }

    @Test
    void testGetChannel_static_name_is_exactly_the_same() {
        //given
        ChannelSinkMapping sut = getMapping(STATIC_CHANNEL_CONFIG);
        SinkRecord record = new SinkRecord("topic", 0, null, null, null, null, 0);
        //when
        final String channelName = sut.getChannelName(record);
        //then
        assertEquals(STATIC_CHANNEL_NAME, channelName);
    }

    @Test
    void testGetChannel_channel_name_is_evaluating_patterns() {
        //given
        SinkRecord record = new SinkRecord("myTopic", 0, null, "myKey".getBytes(), null, null, 0);
        final ChannelSinkConnectorConfig connectorConfig = new ChannelSinkConnectorConfig(
            Map.of(
                "channel", "channel_#{key}_#{topic}",
                "client.key", "test-key",
                "client.id", "test-id")
        );
        ChannelSinkMapping sut = getMapping(connectorConfig);

        //when
        final String channelName = sut.getChannelName(record);

        //then
        assertEquals("channel_myKey_myTopic", channelName);
    }

    @Test
    void testGetChannel_throws_if_missing_key_and_not_skipping() {
        SinkRecord record = new SinkRecord("myTopic", 0, null, null, null, null, 0);
        final ChannelSinkConnectorConfig connectorConfig = new ChannelSinkConnectorConfig(
            Map.of(
                "channel", "channel_#{key}_#{topic}",
                "skipOnKeyAbsence", "false",
                "client.key", "test-key",
                "client.id", "test-id"
            )
        );

        ChannelSinkMapping sut = getMapping(connectorConfig);
        assertThrows(IllegalArgumentException.class, () -> {
            sut.getChannelName(record);
        });
    }

    @Test
    void testGetChannel_returns_null_channel_name_if_skipping_missing_keys() {
        SinkRecord record = new SinkRecord("myTopic", 0, null, null, null, null, 0);
        final ChannelSinkConnectorConfig connectorConfig = new ChannelSinkConnectorConfig(
            Map.of(
                "channel", "channel_#{key}_#{topic}",
                "skipOnKeyAbsence", "true",
                "client.key", "test-key",
                "client.id", "test-id"
            )
        );
        ChannelSinkMapping sut = getMapping(connectorConfig);
        assertNull(sut.getChannelName(record));
    }
}
