package com.ably.kafka.connect.client;

import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import static com.ably.kafka.connect.config.ChannelSinkConnectorConfig.*;
import com.ably.kafka.connect.config.ConfigValueEvaluator;
import com.ably.kafka.connect.mapping.ChannelSinkMapping;
import com.ably.kafka.connect.mapping.MessageSinkMapping;
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
import java.util.HashSet;
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
                                  MessageSinkMapping messageSinkMapping, ConfigValueEvaluator configValueEvaluator) {
        this.connectorConfig = connectorConfig;
        this.channelSinkMapping = channelSinkMapping;
        this.messageSinkMapping = messageSinkMapping;
        this.configValueEvaluator = configValueEvaluator;

    }

    @Override
    public void connect() throws ConnectException, AblyException {
        this.restClient = new AblyRest(String.valueOf(connectorConfig.getPassword(CLIENT_KEY)));
    }

    @Override
    public void publishFrom(SinkRecord record) throws ConnectException {

    }

    /**
     * Function that uses the REST API client to send batches.
     * @param records list of sink records.
     * @throws ConnectException
     */
    public void publishBatch(List<SinkRecord> records) throws ConnectException, AblyException {

        BatchSpec batch;
        Set<String> channelIdSet = new HashSet<>();
        List<Message> messages = new ArrayList<>();

        for(SinkRecord record: records) {
            if (shouldSkip(record)) return;

            try {
                final String channelName = channelSinkMapping.getChannelName(record);
                final Message message = messageSinkMapping.getMessage(record);

                channelIdSet.add(channelName);
                messages.add(message);

            } catch (Exception e) {
               logger.error("publishBatch exception translating from sink record", e);
            }

            batch = new BatchSpec(channelIdSet, messages);
            logger.debug("Ably BATCH call");
            sendBatches(batch);
        }
    }

    /**
     * Function to group the Ably Message objects by channel Name.
     * @param sinkRecords
     * @return
     */
    public Map<String, Set<Message>> groupMessages(List<SinkRecord> sinkRecords) {

        HashMap<String, Set<Message>> channelNameToMessagesMap = new HashMap();

        for(SinkRecord record: sinkRecords) {
            if (!shouldSkip(record)){
                final String channelName = channelSinkMapping.getChannelName(record);
                final Message message = messageSinkMapping.getMessage(record);

                Set<Message> messages;
                if(channelNameToMessagesMap.containsKey(channelName)) {
                    // just retrieve the list and add to it.
                    messages = channelNameToMessagesMap.get(channelName);
                } else {
                    // Create a new Set.
                    messages = new HashSet<>();
                }
                messages.add(message);
                channelNameToMessagesMap.put(channelName, messages);
            }
        }

        return channelNameToMessagesMap;
    }

    @Override
    public void stop() {

    }

    protected boolean shouldSkip(SinkRecord record) {
        final boolean skipOnKeyAbsence = connectorConfig.getBoolean(SKIP_ON_KEY_ABSENCE);

        if (skipOnKeyAbsence) {
            final String messageConfig = connectorConfig.getString(MESSAGE_CONFIG);
            final String channelConfig = connectorConfig.getString(CHANNEL_CONFIG);
            final ConfigValueEvaluator.Result messageResult = configValueEvaluator.evaluate(record, messageConfig, true);
            final ConfigValueEvaluator.Result channelResult = configValueEvaluator.evaluate(record, channelConfig, true);

            if (messageResult.shouldSkip() || channelResult.shouldSkip()) {
                logger.warn("Skipping record as record key is not available in a record where the config for either" +
                        " 'message.name' or 'channel' is configured to use #{key} as placeholders {}", record);
                return true;
            }
        }
        return false;
    }

    /**
     * Function that uses the Ably REST API
     * to send records in batches.
     * @param batch
     * @throws AblyException
     */
    private void sendBatches(
            final BatchSpec batch) throws AblyException {
        final HttpCore.RequestBody body = new HttpUtils.JsonRequestBody(batch);
        final Param[] params = new Param[] { new Param("newBatchResponse", "true") };
        final HttpPaginatedResponse response =
                this.restClient.request("POST", "/messages", params, body, null);
        logger.info(
                "Response: " + response.statusCode
                        + " error: " + response.errorCode + " - " + response.errorMessage
        );

    }
}
