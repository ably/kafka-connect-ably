package com.ably.kafka.connect.client;

import com.ably.kafka.connect.offset.OffsetRegistry;
import io.ably.lib.types.AblyException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;

public interface AblyClient {
    /**
     * Publish a batch of Sink records to Ably Batch REST API.
     * @param records
     * @throws ConnectException
     * @throws AblyException
     */
    void publishBatch(List<SinkRecord> records,
                      ErrantRecordReporter dlqReporter,
                      OffsetRegistry offsetRegistryService) throws ConnectException, AblyException;
}

