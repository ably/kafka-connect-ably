package com.ably.kafka.connect.client;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

public interface AblyClient {
    /**
     * Connect to an Ably service.
     * @param suspensionCallback callback to tell the client whether the connection was suspended.
     * @throws ConnectException if the connection fails
     */
    void connect(SuspensionCallback suspensionCallback) throws ConnectException;

    /**
     * Publish a sink record to Ably.
     * @param record the record to publish
     *
     * throws ConnectException if the publish fails for reasons implementors decide.
    * */
    void publishFrom(SinkRecord record) throws ConnectException;
    /**
     * Stop the ability to publish messages to Ably.
     * */
    void stop();
}

