package com.ably.kafka.connect.batch;

import java.util.concurrent.ScheduledThreadPoolExecutor;

public class BatchProcessingExecutor extends ScheduledThreadPoolExecutor {

    public BatchProcessingExecutor(int corePoolSize) {
        super(corePoolSize);
    }
}
