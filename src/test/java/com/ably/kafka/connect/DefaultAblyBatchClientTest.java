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
import java.util.Set;

public class DefaultAblyBatchClientTest {

    @Test
    public void testGroupMessages() {

        final String STATIC_CHANNEL_NAME = "sink-channel";
        final ChannelSinkConnectorConfig connectorConfig = new ChannelSinkConnectorConfig(Map.of("channel",
                STATIC_CHANNEL_NAME, "client.key", "test-key", "client.id", "test-id"));
        final ConfigValueEvaluator configValueEvaluator = new ConfigValueEvaluator();
        final ChannelConfig channelConfig = new DefaultChannelConfig(connectorConfig);
        final ChannelSinkMapping channelSinkMapping = new DefaultChannelSinkMapping(configValueEvaluator, channelConfig);
        final MessageSinkMapping messageSinkMapping = new DefaultMessageSinkMapping(connectorConfig, configValueEvaluator);
        DefaultAblyBatchClient client = new DefaultAblyBatchClient(connectorConfig, channelSinkMapping,
                messageSinkMapping, configValueEvaluator);

        List<SinkRecord> sinkRecords = new ArrayList<SinkRecord>();
        SinkRecord sinkRecord1 = new SinkRecord("greatTopic", 0, Schema.STRING_SCHEMA, "myKey".getBytes(),
                null, null, 0);

        SinkRecord sinkRecord2 = new SinkRecord("greatTopic", 0, Schema.STRING_SCHEMA, "myKey2".getBytes(),
                null, null, 0);

        sinkRecords.add(sinkRecord1);
        sinkRecords.add(sinkRecord2);
        Map<String, Set<Message>> result = client.groupMessages(sinkRecords);

        Assert.assertTrue(result != null);
    }
}
