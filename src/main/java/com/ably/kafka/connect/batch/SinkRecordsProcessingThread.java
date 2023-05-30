//package com.ably.kafka.connect.batch;
//
//import com.ably.kafka.connect.client.BatchSpec;
//import com.ably.kafka.connect.client.DefaultAblyBatchClient;
//import io.ably.lib.types.Message;
//import org.apache.kafka.connect.sink.SinkRecord;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.ConcurrentLinkedQueue;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.ThreadPoolExecutor;
//import java.util.concurrent.TimeUnit;
//
///**
// * A Separate Runnable(Thread) to retrieve the records
// * from the ConcurrentLinkedQueue and call Ably Batch API
// * to send records in batch.
// * Main intention of this thread is not to block put() in SinkTask.
// */
//public class SinkRecordsProcessingThread implements Runnable {
//
//    private static final Logger logger = LoggerFactory.getLogger(SinkRecordsProcessingThread.class);
//
//    final private ConcurrentLinkedQueue<SinkRecord> records;
//    final private DefaultAblyBatchClient client;
//
//    final ThreadPoolExecutor executor;
//
//    final BlockingQueue<Runnable> batchProcessingQueue = new LinkedBlockingQueue<>();
//
//    public SinkRecordsProcessingThread(
//            ConcurrentLinkedQueue<SinkRecord> sinkRecords, DefaultAblyBatchClient ablyClient,
//            int maxThreadPoolSize) {
//        this.records = sinkRecords;
//        this.client = ablyClient;
//        this.executor = new ThreadPoolExecutor(maxThreadPoolSize, maxThreadPoolSize, 30,
//                TimeUnit.SECONDS, batchProcessingQueue,
//                new ThreadPoolExecutor.CallerRunsPolicy());
//    }
//
//    @Override
//    public void run() {
//        // Process the records.
//        if(this.records.size() > 0) {
//            //try {
//                ArrayList<SinkRecord> subsetRecords = new ArrayList<>();
//                while(true) {
//                    final SinkRecord record = this.records.poll();
//                    if(record == null) {
//                        break;
//                    } else {
//                        subsetRecords.add(record);
//                    }
//                }
//
//                if(!subsetRecords.isEmpty()) {
//                    // Sink records are grouped by channel name.
//                    Map<String, List<Message>> groupedMessages = this.client.groupMessagesByChannel(subsetRecords);
//
//                    // Instead of one BatchSpec it should be a list of batchSpec.
//                    groupedMessages.forEach((key, value) -> {
//                        this.executor.execute(new BatchProcessingThread(new BatchSpec(Set.of(key), value), this.client));
//                    });
//                }
//        } else {
//            logger.debug("No records to process");
//        }
//
//    }
//}
