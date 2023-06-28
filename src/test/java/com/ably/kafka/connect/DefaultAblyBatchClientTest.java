package com.ably.kafka.connect;

import com.ably.kafka.connect.client.DefaultAblyBatchClient;
import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.offset.OffsetRegistry;
import com.ably.kafka.connect.offset.OffsetRegistryService;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.ably.lib.types.AblyException;
import io.ably.lib.types.HttpPaginatedResponse;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultAblyBatchClientTest {

    private DefaultAblyBatchClient getClient(ChannelSinkConnectorConfig config) throws AblyException, ChannelSinkConnectorConfig.ConfigException {
        // TODO: add test coverage for DLQ and offset report
        final ErrantRecordReporter dlqReporter = null;
        final OffsetRegistry offsetRegistry = new OffsetRegistryService();
        return new DefaultAblyBatchClient(config, dlqReporter, offsetRegistry);
    }

    @Test
    public void testParseAblyBatchAPIResponse() throws AblyException, ChannelSinkConnectorConfig.ConfigException {
        final String STATIC_CHANNEL_NAME = "channel_#{topic}";
        final ChannelSinkConnectorConfig connectorConfig = new ChannelSinkConnectorConfig(Map.of("channel",
                STATIC_CHANNEL_NAME, "client.key", "test-key", "client.id", "test-id"));
        final DefaultAblyBatchClient client = getClient(connectorConfig);


        HttpPaginatedResponse response = new HttpPaginatedResponse() {
            @Override
            public JsonElement[] items() {
                return new JsonElement[0];
            }

            @Override
            public HttpPaginatedResponse first() throws AblyException {
                return null;
            }

            @Override
            public HttpPaginatedResponse current() throws AblyException {
                return null;
            }

            @Override
            public HttpPaginatedResponse next() throws AblyException {
                return null;
            }

            @Override
            public boolean hasFirst() {
                return false;
            }

            @Override
            public boolean hasCurrent() {
                return false;
            }

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public boolean isLast() {
                return false;
            }
        };
        response.statusCode = 500;

        assertEquals(DefaultAblyBatchClient.AblyBatchResponse.FAILURE,
                client.parseAblyBatchAPIResponse(response));

        response.statusCode = 400;
        assertEquals(DefaultAblyBatchClient.AblyBatchResponse.FAILURE,
                client.parseAblyBatchAPIResponse(response));

    }

    /**
     *
     * @throws AblyException
     */
    @Test
    public void testResponseErrorMessage() throws AblyException, ChannelSinkConnectorConfig.ConfigException {

        // Failure on one channel
        String errorMessage = "{\"successCount\":0,\"failureCount\":1,\"results\":[{\"channel\":\"SERVER5432\",\"error\":{\"message\":\"action not permitted, app = iaDbjw\",\"code\":40160,\"statusCode\":401,\"nonfatal\":false,\"href\":\"https://help.ably.io/error/40160\"}}]}\n";
        // Failure on mutiple channels(multiple objects in results)

        final String STATIC_CHANNEL_NAME = "channel_#{topic}";
        final ChannelSinkConnectorConfig connectorConfig = new ChannelSinkConnectorConfig(Map.of("channel",
                STATIC_CHANNEL_NAME, "client.key", "test-key", "client.id", "test-id"));
        final DefaultAblyBatchClient client = getClient(connectorConfig);
        Map<String, String> channelToErrorMessageMap = new HashMap();
        Set<String> failedMessageIds = client.getFailedChannels(JsonParser.parseString(errorMessage), channelToErrorMessageMap);

        assertTrue(failedMessageIds.size() == 1);
        assertTrue(channelToErrorMessageMap.get("SERVER5432").equals("{\"message\":\"action not permitted, app = iaDbjw\",\"code\":40160,\"statusCode\":401,\"nonfatal\":false,\"href\":\"https://help.ably.io/error/40160\"}"));
        assertEquals(Set.of("SERVER5432"), failedMessageIds);
    }
}
