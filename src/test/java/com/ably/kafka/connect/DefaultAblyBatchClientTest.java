package com.ably.kafka.connect;

import com.ably.kafka.connect.client.DefaultAblyBatchClient;
import com.ably.kafka.connect.config.ChannelConfig;
import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.config.ConfigValueEvaluator;
import com.ably.kafka.connect.config.DefaultChannelConfig;
import com.ably.kafka.connect.mapping.ChannelSinkMapping;
import com.ably.kafka.connect.mapping.DefaultChannelSinkMapping;
import com.ably.kafka.connect.mapping.DefaultMessageSinkMapping;
import com.ably.kafka.connect.mapping.MessageSinkMapping;
import io.ably.lib.types.Message;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultAblyBatchClientTest {

    @Test
    public void testGroupMessagesByChannel() {

        final String STATIC_CHANNEL_NAME = "channel_#{topic}";
        final ChannelSinkConnectorConfig connectorConfig = new ChannelSinkConnectorConfig(Map.of("channel",
                STATIC_CHANNEL_NAME, "client.key", "test-key", "client.id", "test-id"));
        final ConfigValueEvaluator configValueEvaluator = new ConfigValueEvaluator();
        final ChannelConfig channelConfig = new DefaultChannelConfig(connectorConfig);
        final ChannelSinkMapping channelSinkMapping = new DefaultChannelSinkMapping(configValueEvaluator, channelConfig);
        final MessageSinkMapping messageSinkMapping = new DefaultMessageSinkMapping(connectorConfig, configValueEvaluator);
        DefaultAblyBatchClient client = new DefaultAblyBatchClient(connectorConfig, channelSinkMapping,
                messageSinkMapping, configValueEvaluator);

        List<SinkRecord> sinkRecords = new ArrayList<>();
        SinkRecord sinkRecord1 = new SinkRecord("topic1", 0, Schema.STRING_SCHEMA, "myKey".getBytes(),
                null, null, 0);

        SinkRecord sinkRecord2 = new SinkRecord("topic1", 0, Schema.STRING_SCHEMA, "myKey2".getBytes(),
                null, null, 0);

        SinkRecord sinkRecord3 = new SinkRecord("topic2", 1, Schema.STRING_SCHEMA, "myKey3".getBytes(),
                null, null, 0);

        SinkRecord sinkRecord4 = new SinkRecord("topic2", 1, Schema.STRING_SCHEMA, "myKey4".getBytes(),
                null, null, 0);

        sinkRecords.add(sinkRecord1);
        sinkRecords.add(sinkRecord2);
        sinkRecords.add(sinkRecord3);
        sinkRecords.add(sinkRecord4);


        Map<String, List<Message>> result = client.groupMessagesByChannel(sinkRecords);

        Assert.assertTrue(result != null);

        Assert.assertTrue(result.size() == 2);
        Assert.assertTrue(result.containsKey("channel_topic1"));
        Assert.assertTrue(result.containsKey("channel_topic2"));
        Assert.assertTrue(result.get("channel_topic1").size() == 2);
        Assert.assertTrue(result.get("channel_topic2").size() == 2);

    }
}
