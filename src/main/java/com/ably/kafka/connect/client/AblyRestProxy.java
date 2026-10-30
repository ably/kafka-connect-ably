package com.ably.kafka.connect.client;

import io.ably.pubsub.http.HttpCore;
import io.ably.pubsub.http.PubSubHttpClient;
import io.ably.pubsub.server.PubSubServer;
import io.ably.pubsub.types.AblyException;
import io.ably.pubsub.types.ClientOptions;
import io.ably.pubsub.types.HttpPaginatedResponse;
import io.ably.pubsub.types.Param;

/**
 * Proxy for PubSubHttpClient to enable simpler testing.
 */
public interface AblyRestProxy {

    /**
     * Delegates to {@link io.ably.pubsub.http.PubSubHttpClient}
     */
    HttpPaginatedResponse request(
        String method,
        String path,
        Param[] params,
        HttpCore.RequestBody body,
        Param[] headers) throws AblyException;

    /**
     * Construct a proxy that forwards directly to PubSubHttpClient
     */
    @SuppressWarnings("resource")
    static AblyRestProxy forClientOptions(ClientOptions opts) throws AblyException {
        final PubSubHttpClient restClient = PubSubServer.httpClientBuilder(opts).build();
        return restClient::request;
    }
}
