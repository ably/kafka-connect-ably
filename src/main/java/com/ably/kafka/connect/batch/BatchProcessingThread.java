package com.ably.kafka.connect.batch;

import com.ably.kafka.connect.client.DefaultAblyBatchClient;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
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

    private final ErrantRecordReporter dlqReporter;

    public BatchProcessingThread(List<SinkRecord> sinkRecords,
                                 DefaultAblyBatchClient ablyBatchClient,
                                 ErrantRecordReporter dlqReporter) {
        this.records = sinkRecords;
        this.batchClient = ablyBatchClient;
        this.dlqReporter = dlqReporter;
    }
    @Override
    public void run() {
        batchClient.publishBatch(records, this.dlqReporter);
    }
}
