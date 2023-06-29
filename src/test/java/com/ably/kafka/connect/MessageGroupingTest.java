package com.ably.kafka.connect;


import com.ably.kafka.connect.batch.FatalBatchProcessingException;
import com.ably.kafka.connect.batch.MessageGroup;
import com.ably.kafka.connect.batch.MessageGrouper;
import com.ably.kafka.connect.client.BatchSpec;
import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.mapping.RecordMappingException;
import com.ably.kafka.connect.mapping.RecordMappingFactory;
import com.ably.kafka.connect.utils.CapturedDlqError;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link MessageGroup} and {@link MessageGrouper}
 */
public class MessageGroupingTest {

    /**
     * Ensure that records are grouped by channel and the returned MessageGroup returns the appropriate
     * BatchSpec configuration, and the corresponding SinkRecords where needed.
     */
    @Test
    public void testGroupsByChannelName() throws ChannelSinkConnectorConfig.ConfigException {
        final List<CapturedDlqError> errors = Lists.newArrayList();
        final MessageGrouper sut = grouper("channel_#{key}", "msgName", "skip", errors);
        final List<SinkRecord> records = List.of(
            new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 1, null, "msg1", 0),
            new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 2, null, "msg2", 0),
            new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 3, null, "msg3", 0),
            new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 2, null, "msg4", 0),
            new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 3, null, "msg5", 0),
            new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 1, null, "msg6", 0)
        );

        final MessageGroup group = sut.group(records);

        // Not expecting any errors
        assertEquals(Collections.emptyList(), errors);

        // Check that records have been grouped correctly, and order has been preserved
        final List<BatchSpec> specs = group.specs();
        assertEquals(Map.of(
            "channel_1", List.of("msgName/msg1", "msgName/msg6"),
            "channel_2", List.of("msgName/msg2", "msgName/msg4"),
            "channel_3", List.of("msgName/msg3", "msgName/msg5")
        ), flatten(specs));

        // allChannels() should return all channels we have records for
        assertEquals(Set.of("channel_1", "channel_2", "channel_3"), group.allChannels());

        // Check that recordsForChannel can select correct subsets of records
        assertEquals(List.of(
            new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 1, null, "msg1", 0),
            new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 1, null, "msg6", 0)
        ), group.recordsForChannel("channel_1"));
        assertEquals(List.of(
            new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 2, null, "msg2", 0),
            new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 2, null, "msg4", 0)
        ), group.recordsForChannel("channel_2"));
        assertEquals(List.of(
            new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 3, null, "msg3", 0),
            new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 3, null, "msg5", 0)
        ), group.recordsForChannel("channel_3"));
    }


    /**
     * Ensure that message grouping respects the onFailedRecordMapping configuration
     */
    @Test
    public void testRespectsErrorHandlingConfiguration() throws ChannelSinkConnectorConfig.ConfigException {
        final List<SinkRecord> records = List.of(
            new SinkRecord("topic2", 0, Schema.INT32_SCHEMA, 10, null, "msg1", 0),
            new SinkRecord("topic2", 0, null, null, null, "msg2", 0)
        );

        // `skip` configuration should not submit errors to DLQ and only return valid record mappings
        final List<CapturedDlqError> skipErrors = Lists.newArrayList();
        final MessageGrouper skipGrouper = grouper("test_channel", "name_#{key}", "skip", skipErrors);
        final MessageGroup skipGroup = skipGrouper.group(records);
        assertEquals(Collections.emptyList(), skipErrors);
        assertEquals(Map.of(
            "test_channel", List.of("name_10/msg1")
        ), flatten(skipGroup.specs()));

        // `stop` should kill the sink task during processing
        final List<CapturedDlqError> stopErrors = Lists.newArrayList();
        final MessageGrouper stopGrouper = grouper("test_channel", "name_#{key}", "stop", stopErrors);
        assertEquals(Collections.emptyList(), stopErrors);
        assertThrows(FatalBatchProcessingException.class, () -> stopGrouper.group(records));

        // `dlq` should send failed records to the DLQ
        final List<CapturedDlqError> dlqErrors = Lists.newArrayList();
        final MessageGrouper dlqGrouper = grouper("test_channel", "name_#{key}", "dlq", dlqErrors);
        final MessageGroup dlqGroup = dlqGrouper.group(records);
        assertEquals(1, dlqErrors.size());
        assertEquals(records.get(1), dlqErrors.get(0).record);
        assertEquals(RecordMappingException.class, dlqErrors.get(0).error.getClass());
        assertEquals(Map.of(
            "test_channel", List.of("name_10/msg1")
        ), flatten(dlqGroup.specs()));
    }

    /**
     * Ensure that we default to stopping the sink task if DLQ reporting has been requested in
     * configuration but we have no DLQ set up.
     */
    @Test
    public void testDefaultsToStopIfDlqUnavailable() throws ChannelSinkConnectorConfig.ConfigException {
        final List<SinkRecord> records = List.of(
            new SinkRecord("topic3", 0, null, null, null, "msg1", 0)
        );

        final MessageGrouper grouper = grouper("test_channel", "name_#{key}", "dlq", null);
        assertThrows(FatalBatchProcessingException.class, () -> grouper.group(records));
    }

    /**
     * Helper for testing to extract the key information from BatchSpecs. This is because we can't
     * do direct value comparisons on BatchSpecs as they contain Messages, which are not values.
     */
    private Map<String, List<String>> flatten(final List<BatchSpec> specs) {
        final Map<String, List<String>> flatSpecs = new HashMap<>();
        for (BatchSpec spec : specs) {
            assertEquals(1, spec.getChannels().size());
            final List<String> messages = spec.getMessages().stream()
                .map(msg -> String.format("%s/%s", msg.name, msg.data.toString()))
                .collect(Collectors.toList());
            flatSpecs.put(spec.getChannels().iterator().next(), messages);
        }

        return flatSpecs;
    }

    /**
     * Set up a MessageGrouper for unit testing
     */
    private MessageGrouper grouper(
        String channelPattern,
        String messagePattern,
        String failAction,
        @Nullable List<CapturedDlqError> errorSink) throws ChannelSinkConnectorConfig.ConfigException {
        final ChannelSinkConnectorConfig config = new ChannelSinkConnectorConfig(
            Map.of(
                "client.key", "unused",
                "client.id", "unused",
                "channel", channelPattern,
                "message.name", messagePattern,
                "onFailedRecordMapping", failAction
            )
        );
        final RecordMappingFactory factory = new RecordMappingFactory(config);
        final ErrantRecordReporter dlqReporter =
            errorSink == null ? null : (sinkRecord, throwable) -> {
            errorSink.add(new CapturedDlqError(sinkRecord, throwable));
            return Futures.immediateVoidFuture();
        };
        return new MessageGrouper(
            factory.channelNameMapping(),
            factory.messageNameMapping(),
            config.getFailedMappingAction(),
            dlqReporter
        );
    }
}
