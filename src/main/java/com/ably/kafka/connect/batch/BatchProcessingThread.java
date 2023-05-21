package com.ably.kafka.connect.batch;

import com.ably.kafka.connect.client.BatchSpec;
import com.ably.kafka.connect.client.DefaultAblyBatchClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Part of the thread pool, responsibility
 * is to call the Ably BATCH REST API
 * for every batch.(its grouped by channel)
 */
public class BatchProcessingThread implements Runnable{
    private static final Logger logger = LoggerFactory.getLogger(BatchProcessingThread.class);

    private final BatchSpec batch;

    private final DefaultAblyBatchClient batchClient;

    public BatchProcessingThread(BatchSpec batch, DefaultAblyBatchClient ablyBatchClient) {
        this.batch = batch;
        this.batchClient = ablyBatchClient;
    }
    @Override
    public void run() {
        if(batch != null) {
            // Send Batches.
            try {
                logger.debug("Ably BATCH call -Thread(" + Thread.currentThread().getName() + ")");
                batchClient.sendBatches(batch);
            } catch (Exception e) {
                logger.error("Error while sending batch", e);
            }
        }
    }
}
