package com.ably.kafka.connect.client;

import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import static com.ably.kafka.connect.config.ChannelSinkConnectorConfig.*;
import com.ably.kafka.connect.config.ConfigValueEvaluator;
import com.ably.kafka.connect.mapping.ChannelSinkMapping;
import com.ably.kafka.connect.mapping.MessageSinkMapping;
import com.ably.kafka.connect.offset.OffsetRegistryService;
import io.ably.lib.http.HttpCore;
import io.ably.lib.http.HttpUtils;
import io.ably.lib.rest.AblyRest;
import io.ably.lib.types.AblyException;
import io.ably.lib.types.HttpPaginatedResponse;
import io.ably.lib.types.Message;
import io.ably.lib.types.Param;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
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
    public void publishBatch(List<SinkRecord> records, OffsetRegistryService offsetRegistryService) throws ConnectException {

        if(!records.isEmpty()) {
            List<BatchSpec> batchSpecs = new ArrayList<>();
            Map<String, List<Message>> groupedMessages = this.groupMessagesByChannel(records);
            groupedMessages.forEach((key, value) -> {
                batchSpecs.add(new BatchSpec(Set.of(key), value));
            });
            try {
                logger.info("Ably BATCH call -Thread(" + Thread.currentThread().getName() + ")");
                if(this.sendBatches(batchSpecs)) {
                    offsetRegistryService.updateTopicPartitionToOffsetMap(records);
                }
            } catch (Exception e) {
                logger.error("Error while sending batch", e);
            }
        }
    }

    /**
     * Function to group the Ably Message objects by channel Name.
     * @param sinkRecords
     * @return
     */
    public Map<String, List<Message>> groupMessagesByChannel(List<SinkRecord> sinkRecords) {

        HashMap<String, List<Message>> channelNameToMessagesMap = new HashMap();

        for(SinkRecord record: sinkRecords) {
            try {
                if (!shouldSkip(record)) {
                    final String channelName = channelSinkMapping.getChannelName(record);
                    final Message message = messageSinkMapping.getMessage(record);

                    final List<Message> updatedMessages = channelNameToMessagesMap.getOrDefault(channelName, new ArrayList<>());
                    updatedMessages.add(message);
                    channelNameToMessagesMap.put(channelName, updatedMessages);
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
     * @throws AblyException
     */
    public boolean sendBatches(final List<BatchSpec> batches) throws AblyException {
        final HttpCore.RequestBody body = new HttpUtils.JsonRequestBody(batches);
        final Param[] params = new Param[] { new Param("newBatchResponse", "true") };
        final HttpPaginatedResponse response =
                this.restClient.request("POST", "/messages", params, body, null);
        logger.info(
                "Response: " + response.statusCode
                        + " error: " + response.errorCode + " - " + response.errorMessage
        );

        return true;

    }
}
