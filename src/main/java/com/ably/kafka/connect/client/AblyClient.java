package com.ably.kafka.connect.client;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;

public interface AblyClient {
    void connect() throws ConnectException;
    void publishFrom(SinkRecord message) throws ConnectException, RetriableException;
    void stop();
    boolean isConnected();
}

