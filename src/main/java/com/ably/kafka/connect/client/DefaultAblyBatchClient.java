package com.ably.kafka.connect.client;

import com.ably.kafka.connect.batch.MessageTransformer;
import com.ably.kafka.connect.batch.RecordMessagePair;
import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.mapping.RecordMappingFactory;
import com.ably.kafka.connect.offset.OffsetRegistry;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.ably.lib.http.HttpCore;
import io.ably.lib.http.HttpUtils;
import io.ably.lib.types.AblyException;
import io.ably.lib.types.ErrorInfo;
import io.ably.lib.types.HttpPaginatedResponse;
import io.ably.lib.types.Param;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Ably Batch client based on REST API
 */
public class DefaultAblyBatchClient implements AblyBatchClient {
    private static final Logger logger = LoggerFactory.getLogger(DefaultAblyBatchClient.class);

    protected final ChannelSinkConnectorConfig connectorConfig;
    private final AblyRestProxy restClient;
    @Nullable private final ErrantRecordReporter dlqReporter;
    private final OffsetRegistry offsetRegistryService;
    private final MessageTransformer messageTransformer;

    public DefaultAblyBatchClient(
        final ChannelSinkConnectorConfig connectorConfig,
        @Nullable final ErrantRecordReporter dlqReporter,
        final OffsetRegistry offsetRegistryService
    ) throws AblyException, ChannelSinkConnectorConfig.ConfigException {
        this(
            connectorConfig,
            AblyRestProxy.forClientOptions(connectorConfig.clientOptions),
            dlqReporter,
            offsetRegistryService
        );
    }

    public DefaultAblyBatchClient(
        final ChannelSinkConnectorConfig connectorConfig,
        final AblyRestProxy ablyProxy,
        @Nullable final ErrantRecordReporter dlqReporter,
        final OffsetRegistry offsetRegistryService
    ) throws ChannelSinkConnectorConfig.ConfigException {
        this.connectorConfig = connectorConfig;
        this.restClient = ablyProxy;
        this.dlqReporter = dlqReporter;
        this.offsetRegistryService = offsetRegistryService;

        final RecordMappingFactory mappingFactory = new RecordMappingFactory(this.connectorConfig);
        this.messageTransformer = new MessageTransformer(
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
            final List<RecordMessagePair> recordMessagePairs = this.messageTransformer.transform(records);
            final List<BatchSpec> batchSpecs = recordMessagePairs
                .stream()
                .map(RecordMessagePair::getBatchSpec)
                .collect(Collectors.toList());

            try {
                logger.debug("Ably Batch API call - Thread({})", Thread.currentThread().getName());
                final HttpPaginatedResponse response = this.sendBatches(batchSpecs);

                if (isError(response)) {
                    for (final RecordMessagePair recordMessagePair : recordMessagePairs) {
                        final String channelName = recordMessagePair.getChannelName();
                        final AblyChannelPublishException error = new AblyChannelPublishException(
                            channelName,
                            response.errorCode,
                            response.statusCode,
                            response.errorMessage
                        );
                        sendMessagesToDlq(Collections.singletonList(recordMessagePair.getKafkaRecord()), error);
                    }
                } else {
                    handlePartialFailure(response, recordMessagePairs);
                    offsetRegistryService.updateTopicPartitionToOffsetMap(records);
                }
            } catch (AblyException e) {
                logger.error("Error while sending batch", e);
                sendMessagesToDlq(records, e);
            }
        }
    }

    /**
     * Check to see if the batch response is a complete failure
     */
    private boolean isError(final HttpPaginatedResponse response) {
        return (response.statusCode / 100) == 4 || (response.statusCode / 100) == 5;
    }

    /**
     * If response contains any failed channels, send their corresponding Sink records to the DLQ.
     */
    private void handlePartialFailure(
        HttpPaginatedResponse response,
        List<RecordMessagePair> recordMessagePairs) throws AblyException {
        Iterator<RecordMessagePair> recordMessagePairItr = recordMessagePairs.iterator();
        Iterator<JsonElement> itemsItr = Arrays.stream(response.items()).iterator();

        while (recordMessagePairItr.hasNext() && itemsItr.hasNext()) {
            RecordMessagePair recordMessagePair = recordMessagePairItr.next();
            JsonObject batchSpecResponse = itemsItr.next().getAsJsonObject();

            int failureCount = batchSpecResponse.
                getAsJsonPrimitive("failureCount").getAsInt();

            if (failureCount == 0) continue;

            JsonArray results = batchSpecResponse.getAsJsonObject().getAsJsonArray("results");

            if (results == null || results.size() != 1)
                throw AblyException.fromErrorInfo(new ErrorInfo("Inconsistent batch response: result field should contain single element", 500, 50000));

            JsonObject result = results.iterator().next().getAsJsonObject();
            String channelName = result.getAsJsonPrimitive("channel").getAsString();

            if (result.has("error")) {
                JsonObject error = result.getAsJsonObject("error");
                int errorCode = error.getAsJsonPrimitive("code").getAsInt();
                int statusCode = error.getAsJsonPrimitive("statusCode").getAsInt();
                String errorMessage = error.getAsJsonPrimitive("message").getAsString();
                AblyChannelPublishException publishError =
                    new AblyChannelPublishException(channelName, errorCode, statusCode, errorMessage);
                logger.debug("Submission to channel {} failed", channelName, publishError);
                sendMessagesToDlq(Collections.singletonList(recordMessagePair.getKafkaRecord()), publishError);
            }
        }
    }

    /**
     * Send given records to the DLQ, if it's configured, else do nothing.
     *
     * @param records Failed records to send to the DLQ
     * @param errorMessage the error to associate with these records
     */
    private void sendMessagesToDlq(
        final List<SinkRecord> records,
        final Throwable errorMessage) {
        if (dlqReporter != null) {
            for (final SinkRecord record : records) {
                dlqReporter.report(record, errorMessage);
            }
        }
    }

    /**
     * Function that uses the Ably REST API to send records in batches.
     *
     * @param batches BatchSpecs to send to Ably for publishing
     * @return response from the Ably batch client
     * @throws AblyException if request fails
     */
    private HttpPaginatedResponse sendBatches(final List<BatchSpec> batches) throws AblyException {

        final HttpCore.RequestBody body = new HttpUtils.JsonRequestBody(batches);
        final Param[] params = new Param[]{new Param("newBatchResponse", "true")};
        final HttpPaginatedResponse response =
                this.restClient.request("POST", "/messages", params, body, null);
        logger.debug("Response statusCode={}, errorCode={}, errorMessage={}",
            response.statusCode, response.errorCode, response.errorMessage);

        return response;
    }
}
