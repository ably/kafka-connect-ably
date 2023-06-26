package com.ably.kafka.connect.client;

import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import static com.ably.kafka.connect.config.ChannelSinkConnectorConfig.*;
import com.ably.kafka.connect.config.ConfigValueEvaluator;
import com.ably.kafka.connect.mapping.ChannelSinkMapping;
import com.ably.kafka.connect.mapping.MessageSinkMapping;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import io.ably.lib.http.HttpCore;
import io.ably.lib.http.HttpUtils;
import io.ably.lib.rest.AblyRest;
import io.ably.lib.types.AblyException;
import io.ably.lib.types.HttpPaginatedResponse;
import io.ably.lib.types.Message;
import io.ably.lib.types.Param;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Ably Batch client based on REST API
 */
public class DefaultAblyBatchClient implements AblyClient {
    private static final Logger logger = LoggerFactory.getLogger(DefaultAblyBatchClient.class);

    protected final ChannelSinkMapping channelSinkMapping;
    protected final MessageSinkMapping messageSinkMapping;
    protected final ChannelSinkConnectorConfig connectorConfig;

    private final ConfigValueEvaluator configValueEvaluator;
    AblyRest restClient;

    // Error Codes.
    private static final String ABLY_REST_API_ERROR_CODE_4XX = "4";
    private static final String ABLY_REST_API_ERROR_CODE_5XX = "5";
    private static final String ABLY_REST_API_ERROR_CODE_2XX = "2";

    public enum AblyBatchResponse {
        SUCCESS,
        PARTIAL_FAILURE,
        FAILURE
    }
    public DefaultAblyBatchClient(ChannelSinkConnectorConfig connectorConfig, ChannelSinkMapping channelSinkMapping,
                                  MessageSinkMapping messageSinkMapping, ConfigValueEvaluator configValueEvaluator) throws AblyException {
        this.connectorConfig = connectorConfig;
        this.channelSinkMapping = channelSinkMapping;
        this.messageSinkMapping = messageSinkMapping;
        this.configValueEvaluator = configValueEvaluator;
        this.restClient = new AblyRest(connectorConfig.getPassword(CLIENT_KEY).value());
    }

    /**
     * Function that uses the REST API client to send batches.
     * @param records list of sink records.
     * @throws ConnectException
     */
    @Override
    public void publishBatch(List<SinkRecord> records, ErrantRecordReporter dlqReporter) throws ConnectException {

        if(!records.isEmpty()) {
            List<BatchSpec> batchSpecs = new ArrayList<>();
            Map<String, List<SinkRecord>> channelNameToSinkRecordsMap = new HashMap<>();

            Map<String, List<Message>> groupedMessages = this.groupMessagesByChannel(records, channelNameToSinkRecordsMap);
            groupedMessages.forEach((key, value) -> {
                batchSpecs.add(new BatchSpec(Set.of(key), value));
            });
            try {
                logger.info("Ably BATCH call -Thread(" + Thread.currentThread().getName() + ")");

                // Step 1: Send the batch to Ably.
                HttpPaginatedResponse response = this.sendBatches(batchSpecs);

                // Step 2: Parse the response.
                // If its 4xx or 5xx, then DLQ all the sink records.
                // If its 2xx, then DLQ only the failed sink records for that specific channel.
                AblyBatchResponse ablyBatchResponse = parseAblyBatchAPIResponse(response);

                if(ablyBatchResponse == AblyBatchResponse.FAILURE) {
                    sendMessagesToDLQ(records, dlqReporter);
                } else if(ablyBatchResponse == AblyBatchResponse.PARTIAL_FAILURE) {
                    handlePartialFailure(response, channelNameToSinkRecordsMap, dlqReporter);
                }

            } catch (Exception e) {
                logger.error("Error while sending batch", e);
                sendMessagesToDLQ(records, dlqReporter);
            }
        }
    }

    /**
     * Function to handle partial failure
     * @param response Ably response
     * @param records  Sink records
     * @param dlqReporter DLQ reporter
     */
    public void handlePartialFailure(HttpPaginatedResponse response,
                                     Map<String, List<SinkRecord>> records,
                                     ErrantRecordReporter dlqReporter) {

        Set<String> failedChannels = new HashSet<>();
        JsonElement[] elements = response.items();
        for (JsonElement element : elements) {
            failedChannels.addAll(getFailedChannels(element));
        }

        for(String channel : failedChannels) {
            List<SinkRecord> failedRecords = records.get(channel);
            if(failedRecords != null && !failedRecords.isEmpty()) {
                sendMessagesToDLQ(failedRecords, dlqReporter);
            }
        }
    }

    /**
     * Function to parse Ably Batch API response
     * @param response Ably response
     * @return AblyBatchResponse
     */
    public AblyBatchResponse parseAblyBatchAPIResponse(HttpPaginatedResponse response) {

        AblyBatchResponse ablyBatchResponse = AblyBatchResponse.SUCCESS;

        String statusCode = Integer.toString(response.statusCode);
        if(statusCode.startsWith(ABLY_REST_API_ERROR_CODE_4XX) || statusCode.startsWith(ABLY_REST_API_ERROR_CODE_5XX)) {
            // DLQ all the sink records.
            logger.error("Ably Batch API call failed with error code: " + statusCode);
            ablyBatchResponse = AblyBatchResponse.FAILURE;
        } else if(statusCode.startsWith(ABLY_REST_API_ERROR_CODE_2XX)) {
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
     * Function to send sink records to DLQ.
     * @param records
     * @param dlqReporter
     */
    private void sendMessagesToDLQ(List<SinkRecord> records,
                                   ErrantRecordReporter dlqReporter) {
        if(dlqReporter == null) {
            logger.error("Dead letter queue not configured");
            return;
        }
        for(SinkRecord record: records) {
            dlqReporter.report(record, new Throwable("Ably Batch call failed"));
        }
    }

    /**
     * Function to group the Ably Message objects by channel Name.
     * @param sinkRecords List of Sink Records
     * @return Map of channel name to list of messages.
     */
    public Map<String, List<Message>> groupMessagesByChannel(List<SinkRecord> sinkRecords,
                                                             Map<String, List<SinkRecord>>
                                                                     channelNameToSinkRecordsMap) {

        HashMap<String, List<Message>> channelNameToMessagesMap = new HashMap();

        for(SinkRecord record: sinkRecords) {
            try {
                if (!shouldSkip(record)) {
                    final String channelName = channelSinkMapping.getChannelName(record);
                    final Message message = messageSinkMapping.getMessage(record);

                    final List<Message> updatedMessages = channelNameToMessagesMap.getOrDefault(channelName, new ArrayList<>());
                    updatedMessages.add(message);
                    channelNameToMessagesMap.put(channelName, updatedMessages);

                    final List<SinkRecord> updatedSinkRecords = channelNameToSinkRecordsMap.getOrDefault(channelName, new ArrayList<>());
                    updatedSinkRecords.add(record);
                    channelNameToSinkRecordsMap.put(channelName, updatedSinkRecords);
                }
            } catch(Exception e) {
                logger.error("Error transforming sink record", e);
            }
        }

        return channelNameToMessagesMap;
    }

    protected boolean shouldSkip(SinkRecord record) {
        final boolean skipOnKeyAbsence = connectorConfig.getBoolean(SKIP_ON_KEY_ABSENCE);

        if (skipOnKeyAbsence) {
            final String messageConfig = connectorConfig.getString(MESSAGE_CONFIG);
            final String channelConfig = connectorConfig.getString(CHANNEL_CONFIG);
            final ConfigValueEvaluator.Result messageResult = configValueEvaluator.evaluate(record, messageConfig, true);
            final ConfigValueEvaluator.Result channelResult = configValueEvaluator.evaluate(record, channelConfig, true);

            if (messageResult.shouldSkip() || channelResult.shouldSkip()) {
                logger.debug("Skipping record as record key is not available in a record where the config for either" +
                        " 'message.name' or 'channel' is configured to use #{key} as placeholders {}", record);
                return true;
            }
        }
        return false;
    }

    /**
     * Function that uses the Ably REST API
     * to send records in batches.
     * @param batches
     * @return true if non-retriable, false otherwise
     * @throws AblyException
     */
    public HttpPaginatedResponse sendBatches(final List<BatchSpec> batches) throws AblyException {
        final HttpCore.RequestBody body = new HttpUtils.JsonRequestBody(batches);
        final Param[] params = new Param[]{new Param("newBatchResponse", "true")};
        final HttpPaginatedResponse response =
                this.restClient.request("POST", "/messages", params, body, null);
        logger.info(
                "Response: " + response.statusCode
                        + " error: " + response.errorCode + " - " + response.errorMessage
        );

        return response;
    }

    /**
     * Function to parse the able response message
     * and retrieve the list of failed channel ids.
     * @param element
     */
    public Set<String> getFailedChannels(JsonElement element) {

        Set<String> failedChannels = new HashSet<>();

        int failureCount = element.getAsJsonObject().
                getAsJsonPrimitive("failureCount").getAsInt();

        if(failureCount > 0) {
            JsonArray results = element.getAsJsonObject().getAsJsonArray("results");
            if (results != null) {
                Iterator<JsonElement> iterator = results.iterator();
                while (iterator.hasNext()) {
                    JsonElement resultElement = iterator.next();
                    String channelName = resultElement.getAsJsonObject().
                            getAsJsonPrimitive("channel").getAsString();
                    failedChannels.add(channelName);
                }
            }
        }
        return failedChannels;
    }
}
