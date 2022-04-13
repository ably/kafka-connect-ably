package com.ably.kafka.connect.client;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

public interface AblyClient {
    void connect() throws ConnectException;
    void publishFrom(SinkRecord record) throws ConnectException;
    void stop();
    boolean isConnected();
}

