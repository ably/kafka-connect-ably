package com.ably.kafka.connect;

import com.ably.kafka.connect.client.AblyBatchClient;
import com.ably.kafka.connect.client.AblyChannelPublishException;
import com.ably.kafka.connect.client.AblyRestProxy;
import com.ably.kafka.connect.client.DefaultAblyBatchClient;
import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.offset.OffsetRegistry;
import com.ably.kafka.connect.offset.OffsetRegistryService;
import com.ably.kafka.connect.utils.CapturedDlqError;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.ably.lib.types.AblyException;
import io.ably.lib.types.HttpPaginatedResponse;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AblyBatchClientTest {

    @SuppressWarnings("deprecation")
    private static final JsonParser gson = new JsonParser();

    /**
     * Ensure that batches are sent to the Ably Batch API in the expected structure,
     * with messages in the order they arrived from Kafka. Successful response should
     * cause nothing to be sent to the DLQ. Offset registry should be updated with
     * latest offsets from records
     */
    @Test
    public void testSendsAblyBatchApiRequests() throws ChannelSinkConnectorConfig.ConfigException {
        final List<CapturedDlqError> errors = Lists.newArrayList();
        final List<SinkRecord> records = List.of(
            new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 1, null, "msg1", 0),
            new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 2, null, "msg2", 1),
            new SinkRecord("topic1", 1, Schema.INT32_SCHEMA, 2, null, "msg3", 4),
            new SinkRecord("topic1", 1, Schema.INT32_SCHEMA, 1, null, "msg4", 3)
        );
        final OffsetRegistry registry = new OffsetRegistryService();

        final AblyBatchClient client = getClient(
            "channel_#{key}",
            "static_name",
            "stop", // not important -- no mapping failures expected
            errors,
            registry,
            proxyExpecting(
                // request:
                "[\n" +
                "    {\n" +
                "        channels: ['channel_1'],\n" +
                "        messages: [\n" +
                "            {data: 'msg1', name: 'static_name'}\n" +
                "        ]\n" +
                "    },\n" +
                "    {\n" +
                "        channels: ['channel_2'],\n" +
                "        messages: [\n" +
                "            {data: 'msg2', name: 'static_name'}\n" +
                "        ]\n" +
                "    },\n" +
                "    {\n" +
                "        channels: ['channel_2'],\n" +
                "        messages: [\n" +
                "            {data: 'msg3', name:'static_name'}\n" +
                "        ]\n" +
                "    },\n" +
                "    {\n" +
                "        channels: ['channel_1'],\n" +
                "        messages: [\n" +
                "            {data: 'msg4', name:'static_name'}\n" +
                "        ]\n" +
                "    }\n" +
                "]\n",
                // response:
                201,
                0,
                "", // no error message
                "[\n" +
                    "    {\n" +
                    "        successCount: 1, \n" +
                    "        failureCount: 0,\n" +
                    "        results: [\n" +
                    "            {channel: \"channe_1\", messageId: \"1\"}\n" +
                    "        ]\n" +
                    "    },\n" +
                    "    {\n" +
                    "        successCount: 1, \n" +
                    "        failureCount: 0,\n" +
                    "        results: [\n" +
                    "            {channel: \"channe_2\", messageId: \"2\"}\n" +
                    "        ]\n" +
                    "    },\n" +
                    "    {\n" +
                    "        successCount: 1, \n" +
                    "        failureCount: 0,\n" +
                    "        results: [\n" +
                    "            {channel: \"channe_2\", messageId: \"3\"}\n" +
                    "        ]\n" +
                    "    },\n" +
                    "    {\n" +
                    "        successCount: 1, \n" +
                    "        failureCount: 0,\n" +
                    "        results: [\n" +
                    "            {channel: \"channe_1\", messageId: \"4\"}\n" +
                    "        ]\n" +
                    "    }\n" +
                    "]\n"
            )
        );

        client.publishBatch(records);

        // check nothing was posted to the DLQ
        assertEquals(Collections.emptyList(), errors);

        // check that the offset registry is up-to-date
        final Map<TopicPartition, OffsetAndMetadata> offsets = registry.updateOffsets(
            Map.of(
                // deliberately pass more recent offsets here, to simulated uncommitted records
                // we should still only expect to see the offsets submitted above
                new TopicPartition("topic1", 0), new OffsetAndMetadata(5),
                new TopicPartition("topic1", 1), new OffsetAndMetadata(5)
            )
        );
        assertEquals(Map.of(
            new TopicPartition("topic1", 0), new OffsetAndMetadata(2),
            new TopicPartition("topic1", 1), new OffsetAndMetadata(5)
        ), offsets);
    }

    /**
     * If the whole submission fails and we get an error response at the top level, we
     * should see all messages sent to the DLQ.
     */
    @Test
    public void testSendsEntireBatchToDlqOnFullFailure() throws ChannelSinkConnectorConfig.ConfigException {
        final List<CapturedDlqError> errors = Lists.newArrayList();
        final List<SinkRecord> records = List.of(
            new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 1, null, "msg1", 10),
            new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 2, null, "msg2", 11)
        );
        final OffsetRegistry registry = new OffsetRegistryService();

        final AblyBatchClient client = getClient(
            "static_channel",
            "static_name_2",
            "dlq",
            errors,
            registry,
            proxyExpecting(
                // request:
                "[\n" +
                "    {\n" +
                "        channels: ['static_channel'],\n" +
                "        messages: [\n" +
                "            {data: 'msg1', name: 'static_name_2'}\n" +
                "        ]\n" +
                "    },\n" +
                "    {\n" +
                "        channels: ['static_channel'],\n" +
                "        messages: [\n" +
                "            {data: 'msg2', name:'static_name_2'}\n" +
                "        ]\n" +
                "    }\n" +
                "]\n",
                // response:
                401,
                40140,
                "Token expired",
                "{\n" +
                "  \"error\": {\n" +
                "    \"message\":\"Token expired\",\n" +
                "    \"statusCode\":401,\n" +
                "    \"code\":40140\n" +
                "  }\n" +
                "}"
            )
        );
        client.publishBatch(records);

        // All records should be in the DLQ with error message attached
        final AblyChannelPublishException expectedError =
            new AblyChannelPublishException("static_channel", 40140, 401, "Token expire");
        assertEquals(records.stream()
            .map(record -> new CapturedDlqError(record, expectedError))
            .collect(Collectors.toList()),
            errors);

        // Offset registry should not have been updated
        assertEquals(Collections.emptyMap(), registry.updateOffsets(
            Map.of(
                new TopicPartition("topic1", 0), new OffsetAndMetadata(20)
            )
        ));
    }

    /**
     * If the Ably SDK throws an exception while attempting to publish a batch,
     * we should DLQ the records
     */
    @Test
    public void testPublishExceptionWillDlqRecords() throws ChannelSinkConnectorConfig.ConfigException {
        final List<CapturedDlqError> errors = Lists.newArrayList();
        final List<SinkRecord> records = List.of(
            new SinkRecord("topic3", 0, Schema.INT32_SCHEMA, 1, null, "msg", 10)
        );
        final OffsetRegistry registry = new OffsetRegistryService();
        final AblyBatchClient client = getClient(
            "static_channel",
            "static_name_2",
            "dlq",
            errors,
            registry,
            (method, path, params, body, headers) -> {
                throw AblyException.fromThrowable(new Exception("test"));
            }
        );

        client.publishBatch(records);

        // Record should now be in the DLQ
        assertEquals(1, errors.size());
        assertEquals(records.get(0), errors.get(0).record);
        assertEquals(AblyException.class, errors.get(0).error.getClass());

        // Offset registry should not have been updated
        assertEquals(Collections.emptyMap(), registry.updateOffsets(
            Map.of(
                new TopicPartition("topic3", 0), new OffsetAndMetadata(20)
            )
        ));
    }

    /**
     * Ensure that a partial success from a Batch publish causes only the
     * failed records to be sent to the DLQ
     */
    @Test
    public void testPartialFailureSendsFailedRecordsToDlq() throws ChannelSinkConnectorConfig.ConfigException {
        final List<CapturedDlqError> errors = Lists.newArrayList();
        final List<SinkRecord> records = List.of(
            new SinkRecord("topic1", 0, Schema.INT32_SCHEMA, 1, null, "msg1", 0),
            new SinkRecord("topic1", 1, Schema.INT32_SCHEMA, 2, null, "msg2", 1)
        );
        final OffsetRegistry registry = new OffsetRegistryService();

        final AblyBatchClient client = getClient(
            "channel_#{key}_#{topic.partition}",
            "static_name_3",
            "stop", // not important -- no mapping failures expected
            errors,
            registry,
            proxyExpecting(
                // request:
                "[\n" +
                "    {\n" +
                "        channels: ['channel_1_0'],\n" +
                "        messages: [\n" +
                "            {data: 'msg1', name: 'static_name_3'}\n" +
                "        ]\n" +
                "    },\n" +
                "    {\n" +
                "        channels: ['channel_2_1'],\n" +
                "        messages: [\n" +
                "            {data: 'msg2', name: 'static_name_3'}\n" +
                "        ]\n" +
                "    }\n" +
                "]\n",
                // response:
                201,
                0,
                "", // no error message
                "[\n" +
                "    {\n" +
                "        successCount: 1, \n" +
                "        failureCount: 0,\n" +
                "        results: [\n" +
                "            {channel: \"channe_1_0\", messageId: \"1\"}\n" +
                "        ]\n" +
                "    },\n" +
                "    {\n" +
                "        successCount: 0, \n" +
                "        failureCount: 1,\n" +
                "        results: [\n" +
                "          {\n" +
                "            channel: 'channel_2_1',\n" +
                "            error: {\n" +
                "              message: 'Given credentials do not have the required capability',\n" +
                "              statusCode: 401,\n" +
                "              code: 40160,\n" +
                "              href: ''\n" +
                "            }\n" +
                "          }\n" +
                "        ]\n" +
                "    }\n" +
                "]\n"
            )
        );

        client.publishBatch(records);

        // Check that only the message going to chanel_2_0 is in the DLQ and
        // that the error information has been attached
        assertEquals(List.of(
            new CapturedDlqError(
                new SinkRecord("topic1", 1, Schema.INT32_SCHEMA, 2, null, "msg2", 1),
                new AblyChannelPublishException(
                    "channel_2_1",
                    40160,
                    401,
                    "Given credentials do not have the required capability"
                )
            )
        ), errors);

        // Offset registry *should* have been updated for both partitions, as we've DLQ'ed failures
        assertEquals(
            Map.of(
                new TopicPartition("topic1", 0), new OffsetAndMetadata(1),
                new TopicPartition("topic1", 1), new OffsetAndMetadata(2)
            ), registry.updateOffsets(
                Map.of(
                    new TopicPartition("topic1", 0), new OffsetAndMetadata(20),
                    new TopicPartition("topic1", 1), new OffsetAndMetadata(20)
                )
            )
        );
    }

    /**
     * Set up a proxy implementation to Ably that expects a given JSON request to
     * the /messages endpoint and returns the given canned response.
     */
    private AblyRestProxy proxyExpecting(
        String requestJson,
        int responseStatus,
        int responseError,
        String errorMessage,
        String responseJson) {
        return (method, path, params, body, headers) -> {
            assertEquals("POST", method);
            assertEquals("/messages", path);
            assertEquals("application/json", body.getContentType());
            assertJsonEquals(requestJson, body.getEncoded());
            return testResponse(responseStatus, responseError, errorMessage, responseJson);
        };
    }

    /**
     * Ensures that the given JSON string parses to an equivalent data structure
     * as the UTF-8 encoded bytes provided
     */
    private void assertJsonEquals(String expectedJson, byte[] actualJsonUtf8) {
        assertJsonEquals(expectedJson, new String(actualJsonUtf8, StandardCharsets.UTF_8));
    }

    /**
     * Ensure that two JSON strings parse to equivalent data structures
     */
    @SuppressWarnings("deprecation")
    private void assertJsonEquals(String expectedJson, String actualJson) {
        assertEquals(
            unordered(gson.parse(expectedJson)),
            unordered(gson.parse(actualJson))
        );
    }

    /**
     * If json element is an array, convert it to a set of its elements so that assertions
     * can be order independent
     */
    private Set<JsonElement> unordered(JsonElement jsonElement) {
        if (jsonElement.isJsonArray()) {
            final JsonArray array = jsonElement.getAsJsonArray();
            final Set<JsonElement> elementSet = new HashSet<>();
            for (JsonElement element : array) {
                elementSet.add(element);
            }
            return elementSet;
        } else {
            throw new IllegalArgumentException("jsonElement is not a collection");
        }
    }

    /**
     * Set up a fake HTTP response from the Ably SDK
     */
    private HttpPaginatedResponse testResponse(
        int statusCode,
        int errorCode,
        String errorMessage,
        String jsonBody) {
        final HttpPaginatedResponse response = new HttpPaginatedResponse() {
            @SuppressWarnings("deprecation")
            @Override
            public JsonElement[] items() {
                final JsonElement json = gson.parse(jsonBody);
                if(!json.isJsonArray()) {
                    return new JsonElement[] { json };
                }
                JsonArray jsonArray = json.getAsJsonArray();
                JsonElement[] items = new JsonElement[jsonArray.size()];
                for(int i = 0; i < items.length; i++) {
                    items[i] = jsonArray.get(i);
                }
                return items;
            }

            @Override public HttpPaginatedResponse first() { return this; }
            @Override public HttpPaginatedResponse current() { return this; }
            @Override
            public HttpPaginatedResponse next() { return null; }
            @Override public boolean hasFirst() { return true; }
            @Override public boolean hasCurrent() { return true; }
            @Override public boolean hasNext() { return false; }
            @Override public boolean isLast() { return true; }
        };

        response.statusCode = statusCode;
        response.errorCode = errorCode;
        response.errorMessage = errorMessage;
        return response;
    }

    /**
     * Set up the system under test with faked out dependencies
     */
    private AblyBatchClient getClient(
        String channelMapping,
        String messageNameMapping,
        String failAction,
        @Nullable List<CapturedDlqError> dlqErrors,
        OffsetRegistry offsetRegistry,
        AblyRestProxy ablyProxy
    ) throws ChannelSinkConnectorConfig.ConfigException {
        final Map<String, String> settings = Map.of(
            "client.id", "unused",
            "client.key", "unused",
            "channel", channelMapping,
            "message.name", messageNameMapping,
            "onFailedRecordMapping", failAction
        );
        final ChannelSinkConnectorConfig config = new ChannelSinkConnectorConfig(settings);
        final ErrantRecordReporter dlqReporter = dlqErrors == null ? null : (record, error) -> {
            dlqErrors.add(new CapturedDlqError(record, error));
            return Futures.immediateVoidFuture();
        };
        return new DefaultAblyBatchClient(config, ablyProxy, dlqReporter, offsetRegistry);
    }
}
