package com.ably.kafka.connect;

import com.ably.kafka.connect.client.DefaultAblyBatchClient;
import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.config.ConfigValueEvaluator;
import com.ably.kafka.connect.mapping.ChannelSinkMapping;
import com.ably.kafka.connect.mapping.DefaultChannelSinkMapping;
import com.ably.kafka.connect.mapping.DefaultMessageSinkMapping;
import com.ably.kafka.connect.mapping.MessageSinkMapping;
import io.ably.lib.types.AblyException;
import io.ably.lib.types.Message;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultAblyBatchClientTest {

    @Test
    public void testGroupMessagesByChannel() throws AblyException {

        final String STATIC_CHANNEL_NAME = "channel_#{topic}";
        final ChannelSinkConnectorConfig connectorConfig = new ChannelSinkConnectorConfig(Map.of("channel",
                STATIC_CHANNEL_NAME, "client.key", "test-key", "client.id", "test-id"));
        final ConfigValueEvaluator configValueEvaluator = new ConfigValueEvaluator();
        final ChannelSinkMapping channelSinkMapping = new DefaultChannelSinkMapping(connectorConfig, configValueEvaluator);
        final MessageSinkMapping messageSinkMapping = new DefaultMessageSinkMapping(connectorConfig, configValueEvaluator);
        DefaultAblyBatchClient client = new DefaultAblyBatchClient(connectorConfig, channelSinkMapping,
                messageSinkMapping, configValueEvaluator);

        SinkRecord record1 = new SinkRecord("topic1", 0, Schema.STRING_SCHEMA, "myKey".getBytes(),
                null, "test1", 0);
        SinkRecord record2 = new SinkRecord("topic1", 0, Schema.STRING_SCHEMA, "myKey2".getBytes(),
                null, "test2", 0);
        SinkRecord record3 = new SinkRecord("topic2", 1, Schema.STRING_SCHEMA, "myKey3".getBytes(),
                null, "test3", 0);
        SinkRecord record4 = new SinkRecord("topic2", 1, Schema.STRING_SCHEMA, "myKey4".getBytes(),
                null, "test4", 0);

        List<SinkRecord> sinkRecords = List.of(record1, record2, record3, record4);

        Map<String, List<Message>> result = client.groupMessagesByChannel(sinkRecords);

        assertTrue(result != null);

        assertTrue(result.size() == 2);
        assertTrue(result.containsKey("channel_topic1"));
        assertTrue(result.containsKey("channel_topic2"));
        assertTrue(result.get("channel_topic1").size() == 2);
        assertTrue(result.get("channel_topic2").size() == 2);

        List<Message> topic1Messages = result.get("channel_topic1");
        List<Message> topic2Messages = result.get("channel_topic2");

        assertTrue(topic1Messages.get(0).data.equals("test1"));
        assertTrue(topic1Messages.get(1).data.equals("test2"));

        assertTrue(topic2Messages.get(0).data.equals("test3"));
        assertTrue(topic2Messages.get(1).data.equals("test4"));

    }
}
