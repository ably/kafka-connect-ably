package com.ably.kafka.connect;

import com.ably.kafka.connect.batch.AutoFlushingBuffer;
import com.ably.kafka.connect.batch.BatchProcessingThread;
import com.ably.kafka.connect.client.AblyClient;
import com.ably.kafka.connect.client.AblyClientFactory;
import com.ably.kafka.connect.client.DefaultAblyClientFactory;
import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.offset.OffsetRegistry;
import com.ably.kafka.connect.offset.OffsetRegistryService;
import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import io.ably.lib.types.AblyException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ChannelSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(ChannelSinkTask.class);

    // Maximum time we're willing to wait in a call to stop() for the thread executor pool to end
    private static final long MAX_THREAD_POOL_SHUTDOWN_DELAY_MS = 10000;

    private final BlockingQueue<Runnable> sinkRecordsQueue = new LinkedBlockingQueue<>();
    private ThreadPoolExecutor executor;

    private AutoFlushingBuffer<SinkRecord> buffer;

    private final OffsetRegistry offsetRegistryService = new OffsetRegistryService();

    @Override
    public void start(Map<String, String> settings) {
        logger.info("Starting Ably channel Sink task");

        final AblyClientFactory ablyClientFactory = new DefaultAblyClientFactory(
            this.context.errantRecordReporter(),
            this.offsetRegistryService);

        AblyClient ablyClient;
        try {
            ablyClient = ablyClientFactory.create(settings);
        } catch (ChannelSinkConnectorConfig.ConfigException | AblyException e) {
            throw new RuntimeException(e);
        }

        final int maxThreadPoolSize = Integer.parseInt(settings.getOrDefault
                (ChannelSinkConnectorConfig.BATCH_EXECUTION_THREAD_POOL_SIZE,
                        ChannelSinkConnectorConfig.BATCH_EXECUTION_THREAD_POOL_SIZE_DEFAULT));

        this.executor = new ThreadPoolExecutor(maxThreadPoolSize, maxThreadPoolSize, 30,
                TimeUnit.SECONDS, sinkRecordsQueue,
                new ThreadPoolExecutor.CallerRunsPolicy());

        final int maxBufferLimit = Integer.parseInt(settings.getOrDefault(ChannelSinkConnectorConfig.BATCH_EXECUTION_MAX_BUFFER_SIZE,
            ChannelSinkConnectorConfig.BATCH_EXECUTION_MAX_BUFFER_SIZE_DEFAULT));

        final long maxBufferDelay = Long.parseLong(settings.getOrDefault(ChannelSinkConnectorConfig.BATCH_EXECUTION_MAX_BUFFER_DELAY_MS,
            ChannelSinkConnectorConfig.BATCH_EXECUTION_MAX_BUFFER_DELAY_MS_DEFAULT));

        // Pass the sink task thread through to each batch worker thread so that they have the
        // option of interrupting the main sink task if an error is encountered that requires
        // complete shutdown of this task.
        final Thread sinkTaskThread = Thread.currentThread();
        this.buffer = new AutoFlushingBuffer<>(maxBufferDelay, maxBufferLimit, batch -> {
            logger.info("SinkTask sending records: {}", batch.size());
            this.executor.execute(
                new BatchProcessingThread(
                    batch,
                    ablyClient,
                    sinkTaskThread)
            );
        });
    }

    public void put(Collection<SinkRecord> records) {
        if(records.size() > 0) {
            logger.debug("SinkTask put (buffering) - Num records: " + records.size());
            this.buffer.addAll(new ArrayList<>(records));
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
        Map<TopicPartition, OffsetAndMetadata> offsets)
        throws RetriableException {
        logger.debug("SinkTask preCommit - Num offsets: " + offsets.size());
        return this.offsetRegistryService.updateOffsets(offsets);
    }

    @Override
    public void stop() {
        logger.info("Stopping Ably channel Sink task");

        if(this.executor != null) {
            this.executor.shutdown();
            try {
                if (!this.executor.awaitTermination(MAX_THREAD_POOL_SHUTDOWN_DELAY_MS, TimeUnit.MILLISECONDS)) {
                    logger.warn("Thread pool still running after {} millis", MAX_THREAD_POOL_SHUTDOWN_DELAY_MS);
                }
            } catch (InterruptedException e) {
                logger.warn("Interrupted while waiting for thread pool shutdown");
            }
        }

    }

    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }
}
