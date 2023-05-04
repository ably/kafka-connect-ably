package com.ably.kafka.connect.client;

import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * Ably Batch client based on REST API
 */
public class DefaultAblyBatchClient extends DefaultAblyClient {
    private static final Logger logger = LoggerFactory.getLogger(DefaultAblyBatchClient.class);

    AblyRest restClient;

    public DefaultAblyBatchClient(ChannelSinkConnectorConfig connectorConfig, ChannelSinkMapping channelSinkMapping,
                                  MessageSinkMapping messageSinkMapping, ConfigValueEvaluator configValueEvaluator) {
        super(connectorConfig, channelSinkMapping, messageSinkMapping, configValueEvaluator);
        this.restClient = null;
    }

    @Override
    public void connect(SuspensionCallback suspensionCallback) throws ConnectException, AblyException {

        this.restClient = new AblyRest(connectorConfig.getString(ChannelSinkConnectorConfig.CLIENT_KEY));

    }

    /**
     * Function that uses the REST API client to send batches.
     * @param records list of sink records.
     * @throws ConnectException
     */
    public void publishBatch(List<SinkRecord> records) throws ConnectException, AblyException {

        BatchSpec batch = null;
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
               // handleAblyException(e);
            }

            batch = new BatchSpec(channelIdSet, messages);
            logger.debug("Ably BATCH call");
            sendBatches(this.restClient, batch);
        }
    }

    private static void sendBatches(
            final AblyRest rest,
            final BatchSpec batch) throws AblyException {
        final HttpCore.RequestBody body = new HttpUtils.JsonRequestBody(batch);
        final Param[] params = new Param[] { new Param("newBatchResponse", "true") };
        final HttpPaginatedResponse response =
                rest.request("POST", "/messages", params, body, null);
        System.out.println(
                "Response: " + response.statusCode
                        + " error: " + response.errorCode + " - " + response.errorMessage
        );
        // Note: items() returns JsonElement[], requires Gson dependency
        //  Arrays.stream(response.items()).forEach(System.out::println);
    }
}
