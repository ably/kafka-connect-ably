package com.ably.kafka.connect.client;

import com.ably.kafka.connect.batch.BatchRecord;
import io.ably.lib.types.AblyException;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.List;

public interface AblyBatchClient {
    /**
     * Publish a batch of Sink records to Ably Batch REST API.
     *
     * @param records SinkRecords to send to Ably
     */
    void publishBatch(List<BatchRecord> records);
}
