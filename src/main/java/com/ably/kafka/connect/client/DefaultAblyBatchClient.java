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
import io.ably.lib.types.Param;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.List;


/**
 * Ably Batch client based on REST API
 */
public class DefaultAblyBatchClient extends DefaultAblyClient {

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

    private static void sendBatches(
            final AblyRest rest,
            final List<BatchSpec> batches) throws AblyException {
        final HttpCore.RequestBody body = new HttpUtils.JsonRequestBody(batches);
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
