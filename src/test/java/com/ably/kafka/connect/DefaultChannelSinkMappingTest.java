package com.ably.kafka.connect;

import io.ably.lib.realtime.AblyRealtime;
import io.ably.lib.realtime.Channel;
import io.ably.lib.types.AblyException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DefaultChannelSinkMappingTest {
    private DefaultChannelSinkMapping SUT;

    //dependencies
    private static final String STATIC_CHANNEL_NAME = "sink-channel";
    private static ChannelSinkConnectorConfig STATIC_CHANNEL_CONFIG = new ChannelSinkConnectorConfig(Map.of("channel", STATIC_CHANNEL_NAME, "client.key", "test-key", "client.id", "test-id"));
    private AblyRealtime ablyRealtime;

    @BeforeEach
    void setUp() throws AblyException {
        SUT = new DefaultChannelSinkMapping(STATIC_CHANNEL_CONFIG);
        ablyRealtime = new AblyRealtime(STATIC_CHANNEL_CONFIG.clientOptions);
    }

    @AfterEach
    void tearDown() {
        ablyRealtime.close();
    }

    @Test
    void testGetChannel_static_name_is_exactly_the_same() throws AblyException, ChannelSinkConnectorConfig.ConfigException {
        SinkRecord record = new SinkRecord("topic", 0, null, null, null, null, 0);
        final Channel channel = SUT.getChannel(record, ablyRealtime);
        assertEquals(STATIC_CHANNEL_NAME, channel.name);
    }
}