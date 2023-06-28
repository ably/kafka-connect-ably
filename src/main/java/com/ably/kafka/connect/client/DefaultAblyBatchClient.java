package com.ably.kafka.connect.client;

import com.ably.kafka.connect.batch.MessageGroup;
import com.ably.kafka.connect.batch.MessageGrouper;
import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.mapping.RecordMappingFactory;
import com.ably.kafka.connect.offset.OffsetRegistry;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import io.ably.lib.http.HttpCore;
import io.ably.lib.http.HttpUtils;
import io.ably.lib.rest.AblyRest;
import io.ably.lib.types.AblyException;
import io.ably.lib.types.HttpPaginatedResponse;
import io.ably.lib.types.Param;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Ably Batch client based on REST API
 */
public class DefaultAblyBatchClient implements AblyClient {
    private static final Logger logger = LoggerFactory.getLogger(DefaultAblyBatchClient.class);

    protected final ChannelSinkConnectorConfig connectorConfig;
    private final AblyRest restClient;
    @Nullable private final ErrantRecordReporter dlqReporter;
    private final OffsetRegistry offsetRegistryService;
    private final MessageGrouper messageGrouper;


    // Error Codes.
    private static final String ABLY_REST_API_ERROR_CODE_4XX = "4";
    private static final String ABLY_REST_API_ERROR_CODE_5XX = "5";
    private static final String ABLY_REST_API_ERROR_CODE_2XX = "2";

    public enum AblyBatchResponse {
        SUCCESS,
        PARTIAL_FAILURE,
        FAILURE
    }

    public DefaultAblyBatchClient(
        final ChannelSinkConnectorConfig connectorConfig,
        @Nullable final ErrantRecordReporter dlqReporter,
        final OffsetRegistry offsetRegistryService
    ) throws AblyException, ChannelSinkConnectorConfig.ConfigException {
        this.connectorConfig = connectorConfig;
        this.restClient = new AblyRest(connectorConfig.clientOptions);
        this.dlqReporter = dlqReporter;
        this.offsetRegistryService = offsetRegistryService;

        final RecordMappingFactory mappingFactory = new RecordMappingFactory(this.connectorConfig);
        this.messageGrouper = new MessageGrouper(
            mappingFactory.channelNameMapping(),
            mappingFactory.messageNameMapping(),
            this.connectorConfig.getFailedMappingAction(),
            dlqReporter
        );
    }

    /**
     * Function that uses the REST API client to send batches.
     *
     * @param records list of sink records.
     */
    @Override
    public void publishBatch(List<SinkRecord> records) {
        if (!records.isEmpty()) {
            final MessageGroup groupedMessages = this.messageGrouper.group(records);
            try {
                logger.debug("Ably Batch API call - Thread({})", Thread.currentThread().getName());
                final HttpPaginatedResponse response = this.sendBatches(groupedMessages.specs());
                final AblyBatchResponse ablyBatchResponse = parseAblyBatchAPIResponse(response);

                if (ablyBatchResponse == AblyBatchResponse.FAILURE) {
                    sendMessagesToDlq(records, new Throwable("TODO"));
                } else if (ablyBatchResponse == AblyBatchResponse.PARTIAL_FAILURE) {
                    handlePartialFailure(response, groupedMessages);
                } else {
                    offsetRegistryService.updateTopicPartitionToOffsetMap(records);
                }
            } catch (Exception e) {
                logger.error("Error while sending batch", e);
                sendMessagesToDlq(records, e);
            }
        }
    }

    /**
     * Function to handle partial failure
     */
    public void handlePartialFailure(
        HttpPaginatedResponse response,
        MessageGroup messageGroup) {
        Map<String, String> channelToErrorMessageMap = new HashMap<>();
        Set<String> failedChannels = new HashSet<>();
        JsonElement[] elements = response.items();
        for (JsonElement element : elements) {
            failedChannels.addAll(getFailedChannels(element, channelToErrorMessageMap));
        }

        for (String channel : failedChannels) {
            List<SinkRecord> failedRecords = messageGroup.recordsForChannel(channel);
            if (failedRecords != null && !failedRecords.isEmpty()) {
                // TODO: attach error messages here
                sendMessagesToDlq(failedRecords, new Throwable(channelToErrorMessageMap.get(channel)));
            }
        }
    }

    /**
     * Function to parse Ably Batch API response
     *
     * @param response Ably response
     * @return AblyBatchResponse
     */
    public AblyBatchResponse parseAblyBatchAPIResponse(HttpPaginatedResponse response) {

        AblyBatchResponse ablyBatchResponse = AblyBatchResponse.SUCCESS;

        String statusCode = Integer.toString(response.statusCode);
        if (statusCode.startsWith(ABLY_REST_API_ERROR_CODE_4XX) || statusCode.startsWith(ABLY_REST_API_ERROR_CODE_5XX)) {
            // DLQ all the sink records.
            logger.error("Ably Batch API call failed with error code: " + statusCode);
            ablyBatchResponse = AblyBatchResponse.FAILURE;
        } else if (statusCode.startsWith(ABLY_REST_API_ERROR_CODE_2XX)) {
            JsonElement[] elements = response.items();
            for (JsonElement element : elements) {
                int failureCount = element.getAsJsonObject().getAsJsonPrimitive("failureCount").getAsInt();
                if (failureCount > 0) {
                    // DLQ only the failed sink records for that specific channel.
                    logger.error("Ably Batch API partial failure, Status Code: " + statusCode +
                        " Failure Count: " + failureCount);
                    ablyBatchResponse = AblyBatchResponse.PARTIAL_FAILURE;
                    break;
                }
            }
        }

        return ablyBatchResponse;
    }

    /**
     * Function to send sink records to DLQ if the DLQ is configured.
     * Else does nothing
     *
     * @param records Failed records to send to the DLQ
     */
    private void sendMessagesToDlq(final List<SinkRecord> records,
                                   final Throwable errorMessage) {
        if (dlqReporter == null) {
            return;
        }
        for (SinkRecord record : records) {
            dlqReporter.report(record, errorMessage);
        }
    }

    /**
     * Function that uses the Ably REST API to send records in batches.
     *
     * @param batches BatchSpecs to send to Ably for publishing
     * @return response from the Ably batch client
     * @throws AblyException if request fails
     */
    public HttpPaginatedResponse sendBatches(final List<BatchSpec> batches) throws AblyException {

        final HttpCore.RequestBody body = new HttpUtils.JsonRequestBody(batches);
        final Param[] params = new Param[]{new Param("newBatchResponse", "true")};
        final HttpPaginatedResponse response =
                this.restClient.request("POST", "/messages", params, body, null);
        logger.debug("Response statusCode={}, errorCode={}, errorMessage={}",
            response.statusCode, response.errorCode, response.errorMessage);

        return response;
    }

    /**
     * Function to parse the Ably response message and retrieve the list of failed channel ids.
     *
     * @param element BatchResponse response object
     */
    public Set<String> getFailedChannels(JsonElement element, Map<String, String> channelToErrorMessageMap) {

        Set<String> failedChannels = new HashSet<>();

        int failureCount = element.getAsJsonObject().
                getAsJsonPrimitive("failureCount").getAsInt();

        if(failureCount > 0) {
            JsonArray results = element.getAsJsonObject().getAsJsonArray("results");
            if (results != null) {
                for (JsonElement resultElement : results) {
                    String channelName = resultElement.getAsJsonObject().
                        getAsJsonPrimitive("channel").getAsString();
                    failedChannels.add(channelName);
                    String errorMessage = resultElement.getAsJsonObject().getAsJsonObject("error").toString();
                    channelToErrorMessageMap.put(channelName, errorMessage);
                }
            }
        }
        return failedChannels;
    }
}
