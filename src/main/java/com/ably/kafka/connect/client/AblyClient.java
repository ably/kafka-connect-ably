package com.ably.kafka.connect.client;

import io.ably.lib.types.AblyException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;

public interface AblyClient {
    /**
     * Publish a batch of Sink records to Ably Batch REST API.
     * @param records
     * @throws ConnectException
     * @throws AblyException
     */
    void publishBatch(List<SinkRecord> records) throws ConnectException, AblyException;
}

