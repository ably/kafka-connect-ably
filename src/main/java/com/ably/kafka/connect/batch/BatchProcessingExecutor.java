package com.ably.kafka.connect.batch;

import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Thread pool executor to offload handling of SinkRecords.
 */
public class BatchProcessingExecutor extends ScheduledThreadPoolExecutor {

    public BatchProcessingExecutor(int corePoolSize) {
        super(corePoolSize);
    }
}
