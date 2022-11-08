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

public class ChannelSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(ChannelSinkTask.class);

    private AblyClientFactory ablyClientFactory = new DefaultAblyClientFactory();
    private AblyClient ablyClient;

    public ChannelSinkTask() {}

    @VisibleForTesting
    ChannelSinkTask(AblyClientFactory factory) {
        this.ablyClientFactory = factory;
    }

    @Override
    public void start(Map<String, String> settings) {
        logger.info("Starting Ably channel Sink task");
        try {
            ablyClient = ablyClientFactory.create(settings);
        } catch (ChannelSinkConnectorConfig.ConfigException e) {
            logger.error("Failed to create Ably client", e);
        }
        ablyClient.connect();
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (ablyClient == null) {
            throw new ConnectException("Ably client is unitialized");
        }

        for (final SinkRecord record : records) {
            ablyClient.publishFrom(record);
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
