package com.ably.kafka.connect.batch;


import com.ably.kafka.connect.client.AblyClient;
import com.ably.kafka.connect.client.DefaultAblyBatchClient;
import com.google.common.collect.Lists;
import io.ably.lib.types.AblyException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A Separate Runnable(Thread) to retrieve the records
 * from the ConcurrentLinkedQueue and call Ably Batch API
 * to send records in batch.
 */
public class BatchProcessingThread implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(BatchProcessingThread.class);

    final private ConcurrentLinkedQueue<SinkRecord> records;
    final private AblyClient client;

    public BatchProcessingThread(
            ConcurrentLinkedQueue<SinkRecord> sinkRecords, AblyClient ablyClient) {
        this.records = sinkRecords;
        this.client = ablyClient;
    }


    @Override
    public void run() {
        // Process the records.
        if(this.records.size() > 0) {
            try {
                ArrayList<SinkRecord> subsetRecords = new ArrayList<>();
                while(true) {
                    final SinkRecord record = this.records.poll();
                    if(record == null) {
                        break;
                    } else {
                        subsetRecords.add(record);
                    }
                }
                this.client.publishBatch(subsetRecords);
            } catch (AblyException e) {
                throw new RuntimeException(e);
            }
        } else {
            logger.debug("No records to process");
        }

    }
}
