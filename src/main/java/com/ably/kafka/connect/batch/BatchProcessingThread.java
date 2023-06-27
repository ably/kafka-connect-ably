package com.ably.kafka.connect.batch;

import com.ably.kafka.connect.client.DefaultAblyBatchClient;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import com.ably.kafka.connect.offset.OffsetRegistry;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Part of the thread pool, responsibility
 * is to call the Ably BATCH REST API
 * for every batch.(its grouped by channel)
 */
public class BatchProcessingThread implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(BatchProcessingThread.class);

    private final Thread mainSinkTask;

    private final List<SinkRecord> records;
    private final DefaultAblyBatchClient batchClient;
    private final ErrantRecordReporter dlqReporter;
    private final OffsetRegistry offsetRegistryService;

    public BatchProcessingThread(
        final List<SinkRecord> sinkRecords,
        final DefaultAblyBatchClient ablyBatchClient,
        final ErrantRecordReporter dlqReporter,
        final OffsetRegistry offsetRegistryService,
        final Thread mainSinkTask) {
        this.records = sinkRecords;
        this.batchClient = ablyBatchClient;
        this.dlqReporter = dlqReporter;
        this.offsetRegistryService = offsetRegistryService;
        this.mainSinkTask = mainSinkTask;
    }

    @Override
    public void run() {
        try {
            batchClient.publishBatch(records, this.dlqReporter, offsetRegistryService);
        } catch (FatalBatchProcessingException e) {
            logger.error("Worker thread killed due to fatal processing error, interrupting SinkTask", e);
            mainSinkTask.interrupt();
        }
    }
}
