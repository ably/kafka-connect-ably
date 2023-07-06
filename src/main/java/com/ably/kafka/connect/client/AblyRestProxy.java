package com.ably.kafka.connect.client;

import io.ably.lib.http.HttpCore;
import io.ably.lib.rest.AblyRest;
import io.ably.lib.types.AblyException;
import io.ably.lib.types.ClientOptions;
import io.ably.lib.types.HttpPaginatedResponse;
import io.ably.lib.types.Param;

/**
 * Proxy for AblyRest to enable simpler testing.
 */
public interface AblyRestProxy {

    /**
     * Delegates to {@link io.ably.lib.rest.AblyRest}
     */
    HttpPaginatedResponse request(
        String method,
        String path,
        Param[] params,
        HttpCore.RequestBody body,
        Param[] headers) throws AblyException;

    /**
     * Construct a proxy that forwards directly to AblyRest
     */
    @SuppressWarnings("resource")
    static AblyRestProxy forClientOptions(ClientOptions opts) throws AblyException {
        final AblyRest restClient = new AblyRest(opts);
        return restClient::request;
    }
}
