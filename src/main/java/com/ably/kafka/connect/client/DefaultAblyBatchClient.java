package com.ably.kafka.connect.client;

import com.ably.kafka.connect.batch.BatchRecord;
import com.ably.kafka.connect.batch.MessageGroup;
import com.ably.kafka.connect.batch.MessageGrouper;
import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.mapping.RecordMappingFactory;
import com.ably.kafka.connect.offset.OffsetRegistry;
import com.ably.kafka.connect.utils.Tracing;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.ably.lib.http.HttpCore;
import io.ably.lib.http.HttpUtils;
import io.ably.lib.types.AblyException;
import io.ably.lib.types.HttpPaginatedResponse;
import io.ably.lib.types.Param;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;

import javax.annotation.Nullable;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.List;

/**
 * Ably Batch client based on REST API
 */
public class DefaultAblyBatchClient implements AblyBatchClient {
    private static final Logger logger = LoggerFactory.getLogger(DefaultAblyBatchClient.class);

    protected final ChannelSinkConnectorConfig connectorConfig;
    private final AblyRestProxy restClient;
    @Nullable private final ErrantRecordReporter dlqReporter;
    private final OffsetRegistry offsetRegistryService;
    private final MessageGrouper messageGrouper;

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
    public void publishBatch(List<BatchRecord> batchRecords) {
        List<SinkRecord> records = batchRecords.stream().map(r -> r.record).collect(Collectors.toList());
        if (!records.isEmpty()) {
            for (BatchRecord record : batchRecords) {
                logger.debug("publishBatch ending queue span");
                record.queueSpan.end();
            }

            List<Span> publishSpans = batchRecords.stream()
                .map(record -> {
                // extract the trace context
                Context context = Tracing.otel.getPropagators().getTextMapPropagator().extract(
                    Context.current(),
                    record.record,
                    Tracing.textMapGetter);

                    // start a publish span
                    Span span = Tracing.tracer.spanBuilder("publish")
                            .setParent(context.with(record.serverSpan))
                            .startSpan();

                    return span;
                })
                .collect(Collectors.toList());

            final MessageGroup groupedMessages = this.messageGrouper.group(records);
            try {
                logger.debug("Ably Batch API call - Thread({})", Thread.currentThread().getName());
                final HttpPaginatedResponse response = this.sendBatches(groupedMessages.specs());

                if (isError(response)) {
                    for (final String channelName : groupedMessages.allChannels()) {
                        final AblyChannelPublishException error = new AblyChannelPublishException(
                            channelName,
                            response.errorCode,
                            response.statusCode,
                            response.errorMessage
                        );
                        sendMessagesToDlq(groupedMessages.recordsForChannel(channelName), error);
                    }
                } else {
                    handlePartialFailure(response, groupedMessages);
                    offsetRegistryService.updateTopicPartitionToOffsetMap(records);
                }
            } catch (AblyException e) {
                logger.error("Error while sending batch", e);
                sendMessagesToDlq(records, e);
            }

            for (Span span : publishSpans) {
                logger.debug("publishBatch ending publish span");
                span.end();
            }
            for (BatchRecord record : batchRecords) {
                logger.debug("publishBatch ending server span");
                record.serverSpan.end();
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
        MessageGroup messageGroup) throws AblyException {

        do {
            JsonElement[] batchResponses = response.items();
            for (JsonElement batchResponse : batchResponses) {
                for (AblyChannelPublishException error : getFailedChannels(batchResponse)) {
                    logger.debug("Submission to channel {} failed", error.channelName, error);
                    final List<SinkRecord> failedRecords = messageGroup.recordsForChannel(error.channelName);
                    sendMessagesToDlq(failedRecords, error);
                }
            }

            response = response.next();
        } while (response != null);
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

    /**
     * Function to parse the Ably response message and retrieve the list of failed channel ids.
     *
     * @param batchResponse BatchResponse response object
     */
    private List<AblyChannelPublishException> getFailedChannels(JsonElement batchResponse) {

        final List<AblyChannelPublishException> failures = Lists.newArrayList();

        final int failureCount = batchResponse.getAsJsonObject().
                getAsJsonPrimitive("failureCount").getAsInt();

        if(failureCount > 0) {
            JsonArray results = batchResponse.getAsJsonObject().getAsJsonArray("results");
            if (results != null) {
                for (JsonElement resultElement : results) {
                    final JsonObject result = resultElement.getAsJsonObject();
                    final String channelName = result.getAsJsonPrimitive("channel").getAsString();
                    if (result.has("error")) {
                        final JsonObject error = result.getAsJsonObject("error");
                        final int errorCode = error.getAsJsonPrimitive("code").getAsInt();
                        final int statusCode = error.getAsJsonPrimitive("statusCode").getAsInt();
                        final String errorMessage = error.getAsJsonPrimitive("message").getAsString();
                        failures.add(new AblyChannelPublishException(channelName, errorCode, statusCode, errorMessage));
                    }
                }
            }
        }

        return failures;
    }
}
