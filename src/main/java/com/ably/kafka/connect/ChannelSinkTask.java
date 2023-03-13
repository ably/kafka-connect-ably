package com.ably.kafka.connect;

import com.ably.kafka.connect.client.AblyClient;
import com.ably.kafka.connect.client.AblyClientFactory;
import com.ably.kafka.connect.client.DefaultAblyClientFactory;
import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChannelSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(ChannelSinkTask.class);

    private AblyClientFactory ablyClientFactory = new DefaultAblyClientFactory();
    private AblyClient ablyClient;
    //in case connection is suspended, sinked messages will be fed to suspend queue
    private final SuspendQueue<SinkRecord> suspendQueue = new SuspendQueue<>();
    private final AtomicBoolean suspended = new AtomicBoolean(false);

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
            ablyClient = ablyClientFactory.create(settings);
        } catch (ChannelSinkConnectorConfig.ConfigException e) {
            logger.error("Failed to create Ably client", e);
        }
        ablyClient.connect(isSuspended -> {
            suspended.set(isSuspended);
            if (!isSuspended){
                processSuspendQueue();
            }
        });
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (ablyClient == null) {
            throw new ConnectException("Ably client is unitialized");
        }

        for (final SinkRecord record : records) {
            publishSingleRecord(record);
        }
    }

    private void publishSingleRecord(SinkRecord record) {
        if (suspended.get()){
            suspendQueue.enqueue(record);
        } else if (!suspendQueue.isNotEmpty()) {
            while (suspendQueue.isNotEmpty() && !suspended.get()){
                //wait for queue to be emptied
            }
            // If connection got into suspended state again add record to the queue, otherwise publish normally
            if (suspended.get()) {
                suspendQueue.enqueue(record);
            } else {
                ablyClient.publishFrom(record);
            }
        } else {
            ablyClient.publishFrom(record);
        }
    }

    private void processSuspendQueue() {
        SinkRecord suspendRecord = null;
        while ((suspendRecord = suspendQueue.dequeue()) != null && !suspended.get()){
            ablyClient.publishFrom(suspendRecord);
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

    }

    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }
}
