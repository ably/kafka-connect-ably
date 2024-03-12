package com.ably.kafka.connect;


import com.ably.kafka.connect.batch.FatalBatchProcessingException;
import com.ably.kafka.connect.batch.MessageTransformer;
import com.ably.kafka.connect.batch.RecordMessagePair;
import com.ably.kafka.connect.client.BatchSpec;
import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.mapping.RecordMappingException;
import com.ably.kafka.connect.mapping.RecordMappingFactory;
import com.ably.kafka.connect.utils.CapturedDlqError;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import io.ably.lib.types.Message;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link RecordMessagePair} and {@link MessageTransformer}
 */
public class MessageGroupingTest {

    /**
     * Ensure that records are grouped by channel and the returned MessageTransformer returns the appropriate
     * BatchSpec configuration, and the corresponding SinkRecords where needed.
     */
    @Test
    public void testGroupsByChannelName() throws ChannelSinkConnectorConfig.ConfigException {
        final List<CapturedDlqError> errors = Lists.newArrayList();
        final MessageTransformer sut = transformer("channel_#{key}", "msgName", "skip", errors);
        final List<SinkRecord> records = List.of(
            new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 1, null, "msg1", 0),
            new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 2, null, "msg2", 0),
            new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 3, null, "msg3", 0),
            new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 2, null, "msg4", 0),
            new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 3, null, "msg5", 0),
            new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 1, null, "msg6", 0)
        );

        final List<RecordMessagePair> recordMessagePairs = sut.transform(records);

        // Not expecting any errors
        assertEquals(Collections.emptyList(), errors);

        // Check that records have been grouped correctly, and order has been preserved
        assertEquals(List.of(
            Map.of("channel_1", "msgName/msg1"),
            Map.of("channel_2", "msgName/msg2"),
            Map.of("channel_3", "msgName/msg3"),
            Map.of("channel_2", "msgName/msg4"),
            Map.of("channel_3", "msgName/msg5"),
            Map.of("channel_1", "msgName/msg6")
        ), flatten(recordMessagePairs));
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
        final MessageTransformer skipTransformer = transformer("test_channel", "name_#{key}", "skip", skipErrors);
        final List<RecordMessagePair> recordMessagePairs = skipTransformer.transform(records);
        assertEquals(Collections.emptyList(), skipErrors);
        assertEquals(List.of(
            Map.of("test_channel", "name_10/msg1")
        ), flatten(recordMessagePairs));

        // `stop` should kill the sink task during processing
        final List<CapturedDlqError> stopErrors = Lists.newArrayList();
        final MessageTransformer stopTransformer = transformer("test_channel", "name_#{key}", "stop", stopErrors);
        assertEquals(Collections.emptyList(), stopErrors);
        assertThrows(FatalBatchProcessingException.class, () -> stopTransformer.transform(records));

        // `dlq` should send failed records to the DLQ
        final List<CapturedDlqError> dlqErrors = Lists.newArrayList();
        final MessageTransformer dlqTransformer = transformer("test_channel", "name_#{key}", "dlq", dlqErrors);
        final List<RecordMessagePair> dlqMessagePairs = dlqTransformer.transform(records);
        assertEquals(1, dlqErrors.size());
        assertEquals(records.get(1), dlqErrors.get(0).record);
        assertEquals(RecordMappingException.class, dlqErrors.get(0).error.getClass());
        assertEquals(List.of(
            Map.of("test_channel", "name_10/msg1")
        ), flatten(dlqMessagePairs));
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

        final MessageTransformer transformer = transformer("test_channel", "name_#{key}", "dlq", null);
        assertThrows(FatalBatchProcessingException.class, () -> transformer.transform(records));
    }

    /**
     * Helper for testing to extract the key information from BatchSpecs. This is because we can't
     * do direct value comparisons on BatchSpecs as they contain Messages, which are not values.
     */
    private List<Map<String, String>> flatten(final List<RecordMessagePair> recordMessagePairs) {
        return recordMessagePairs.stream().map(record -> {
            Message msg = record.getMessage();
            return Map.of(record.getChannelName(), String.format("%s/%s", msg.name, msg.data.toString()));
        }).collect(Collectors.toList());
    }

    /**
     * Set up a MessageTransformer for unit testing
     */
    private MessageTransformer transformer(
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
        return new MessageTransformer(
            factory.channelNameMapping(),
            factory.messageNameMapping(),
            config.getFailedMappingAction(),
            dlqReporter
        );
    }
}
