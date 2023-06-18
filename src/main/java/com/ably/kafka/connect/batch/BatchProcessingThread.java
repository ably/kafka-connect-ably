package com.ably.kafka.connect.batch;

import com.ably.kafka.connect.client.DefaultAblyBatchClient;
import com.ably.kafka.connect.offset.OffsetRegistryService;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Part of the thread pool, responsibility
 * is to call the Ably BATCH REST API
 * for every batch.(its grouped by channel)
 */
public class BatchProcessingThread implements Runnable{

    private final List<SinkRecord> records;

    private final DefaultAblyBatchClient batchClient;

    private final OffsetRegistryService offsetRegistryService;

    public BatchProcessingThread(List<SinkRecord> sinkRecords,
                                 DefaultAblyBatchClient ablyBatchClient,
                                 OffsetRegistryService offsetRegistryService) {
        this.records = sinkRecords;
        this.batchClient = ablyBatchClient;
        this.offsetRegistryService = offsetRegistryService;
    }
    @Override
    public void run() {
        batchClient.publishBatch(records, offsetRegistryService);
    }
}
