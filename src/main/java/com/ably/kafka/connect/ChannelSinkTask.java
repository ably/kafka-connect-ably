package com.ably.kafka.connect;

import com.ably.kafka.connect.batch.AutoFlushingBuffer;
import com.ably.kafka.connect.batch.BatchProcessingThread;
import com.ably.kafka.connect.client.AblyClientFactory;
import com.ably.kafka.connect.client.DefaultAblyBatchClient;
import com.ably.kafka.connect.client.DefaultAblyClientFactory;
import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import io.ably.lib.types.AblyException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
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

    private final AblyClientFactory ablyClientFactory = new DefaultAblyClientFactory();
    private DefaultAblyBatchClient ablyClient;

    private final BlockingQueue<Runnable> sinkRecordsQueue = new LinkedBlockingQueue<>();
    private ThreadPoolExecutor executor;

    private AutoFlushingBuffer<SinkRecord> buffer;

    @Override
    public void start(Map<String, String> settings) {
        logger.info("Starting Ably channel Sink task");

        try {
            this.ablyClient = (DefaultAblyBatchClient) this.ablyClientFactory.create(settings);
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
            logger.info("SinkTask sending records: " + batch.size());
            this.executor.execute(new BatchProcessingThread(batch, this.ablyClient, sinkTaskThread));
        });
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if(records.size() > 0) {
            logger.debug("SinkTask put (buffering) - Num records: " + records.size());
            this.buffer.addAll(new ArrayList<>(records));
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        // Currently irrelevant because the put call is synchronous
        return;
    }

    @Override
    public void stop() {
        logger.info("Stopping Ably channel Sink task");

        if(this.executor != null) {
            this.executor.shutdown();
        }

    }

    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }
}
