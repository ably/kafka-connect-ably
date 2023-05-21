package com.ably.kafka.connect;

import com.ably.kafka.connect.batch.SinkRecordsProcessingThread;
import com.ably.kafka.connect.client.AblyClient;
import com.ably.kafka.connect.client.AblyClientFactory;
import com.ably.kafka.connect.client.DefaultAblyBatchClient;
import com.ably.kafka.connect.client.DefaultAblyClientFactory;
import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.google.common.annotations.VisibleForTesting;
import io.ably.lib.types.AblyException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ChannelSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(ChannelSinkTask.class);

    private AblyClientFactory ablyClientFactory = new DefaultAblyClientFactory();
    private DefaultAblyBatchClient ablyClient;

    private SinkRecordsProcessingThread sinkRecordsProcessingThread = null;

    private ScheduledExecutorService executor = null;

    private ConcurrentLinkedQueue<SinkRecord> sinkRecords = null;

    public ChannelSinkTask() {}

    @VisibleForTesting
    ChannelSinkTask(AblyClientFactory factory) {
        this.ablyClientFactory = factory;
    }

    @VisibleForTesting
    AblyClient getAblyClient() {
        return ablyClient;
    }

    @Override
    public void start(Map<String, String> settings) {
        logger.info("Starting Ably channel Sink task");

        try {
            this.ablyClient = (DefaultAblyBatchClient) this.ablyClientFactory.create(settings);
        } catch (ChannelSinkConnectorConfig.ConfigException | AblyException e) {
            throw new RuntimeException(e);
        }

        int maxThreadPoolSize = Integer.parseInt(settings.getOrDefault
                (ChannelSinkConnectorConfig.BATCH_EXECUTION_THREAD_POOL_SIZE,
        ChannelSinkConnectorConfig.BATCH_EXECUTION_THREAD_POOL_SIZE_DEFAULT));
        // start the Batch processing thread.
        this.sinkRecords = new ConcurrentLinkedQueue<>();
        this.sinkRecordsProcessingThread = new SinkRecordsProcessingThread(this.sinkRecords,
                this.ablyClient, maxThreadPoolSize);
        this.executor =  Executors.newSingleThreadScheduledExecutor();

        this.executor.scheduleAtFixedRate(this.sinkRecordsProcessingThread, 0, Integer.parseInt(settings.getOrDefault
                (ChannelSinkConnectorConfig.BATCH_EXECUTION_FLUSH_TIME,
                        ChannelSinkConnectorConfig.BATCH_EXECUTION_FLUSH_TIME_DEFAULT)), TimeUnit.MILLISECONDS);

    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if(records.size() > 0) {
            logger.debug("SinkTask put - Num records: "+ records.size());
            this.sinkRecords.addAll(records);
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
        ablyClient.stop();

        if(this.executor != null) {
            this.executor.shutdown();
        }

    }

    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }
}
