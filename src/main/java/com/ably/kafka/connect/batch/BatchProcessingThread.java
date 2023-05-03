package com.ably.kafka.connect.batch;


import org.apache.kafka.connect.sink.SinkRecord;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A Separate Runnable(Thread) to retrieve the records
 * from the ConcurrentLinkedQueue and call Ably Batch API
 * to send records in batch.
 */
public class BatchProcessingThread implements Runnable {

    final private ConcurrentLinkedQueue<SinkRecord> records;


    public BatchProcessingThread(ConcurrentLinkedQueue<SinkRecord> sinkRecords) {
        this.records = sinkRecords;
    }


    @Override
    public void run() {

    }
}
