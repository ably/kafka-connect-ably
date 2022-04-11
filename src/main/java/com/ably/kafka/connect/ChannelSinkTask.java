package com.ably.kafka.connect;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import io.ably.lib.realtime.AblyRealtime;
import io.ably.lib.realtime.Channel;
import io.ably.lib.realtime.CompletionListener;
import io.ably.lib.realtime.ConnectionState;
import io.ably.lib.types.AblyException;
import io.ably.lib.types.ErrorInfo;
import io.ably.lib.types.Message;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class ChannelSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(ChannelSinkTask.class);

    private AblyRealtime ably;

    private ChannelSinkMapping channelSinkMapping;
    private MessageSinkMapping messageSinkMapping;

    @Override
    public void start(Map<String, String> settings) {
        logger.info("Starting Ably channel Sink task");

        final ChannelSinkConnectorConfig connectorConfig = new ChannelSinkConnectorConfig(settings);
        final ConfigValueEvaluator configValueEvaluator = new ConfigValueEvaluator();
        final ChannelConfig channelConfig = new DefaultChannelConfig(connectorConfig);
        channelSinkMapping = new DefaultChannelSinkMapping(connectorConfig, configValueEvaluator, channelConfig);
        messageSinkMapping = new MessageSinkMappingImpl(connectorConfig, configValueEvaluator);
        if (connectorConfig.clientOptions == null) {
            logger.error("Ably client options were not initialized due to invalid configuration.");
            return;
        }
        logger.info("Initializing Ably client with key: {}", connectorConfig.clientOptions.key);
        connectorConfig.clientOptions.logHandler = new ClientOptionsLogHandler(logger);

        //We want to wait for connection to be made before processing any sink record
        syncAblyConnect(connectorConfig);
    }

    private void syncAblyConnect(ChannelSinkConnectorConfig connectorConfig) {
        final CountDownLatch connectedSignal = new CountDownLatch(1);
        try {
            ably = new AblyRealtime(connectorConfig.clientOptions);
            ably.connection.on(connectionStateChange -> {
                logger.info("Connection state changed to {}", connectionStateChange.current);
                if (connectionStateChange.current == ConnectionState.failed) {
                    logger.error("Connection failed with error: {}", connectionStateChange.reason);
                    //We want to unblock the thread, the next check point put should handle the error
                    connectedSignal.countDown();
                } else if (connectionStateChange.current == ConnectionState.connected) {
                    logger.info("Ably connection successfully established");
                    connectedSignal.countDown();
                }
            });

            connectedSignal.await();

        } catch (AblyException | InterruptedException e) {
            logger.error("error initializing ably client", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        logger.info("Received {} records", records.size());

        if (ably == null) {
            throw new ConnectException("Ably client is unitialized");
        }

        if (ably.connection.state != ConnectionState.connected) {
            logger.error("Ably client is not connected.");
            throw new ConnectException("Ably client is not connected");
        }

        for (final SinkRecord record : records) {
            try {
                final Channel channel = channelSinkMapping.getChannel(record, ably);
                final Message message = messageSinkMapping.getMessage(record);
                logger.info("Publishing message to channel {}", channel.name);
                logger.info("Message: {}", message);
                channel.publish(message, new CompletionListener() {
                    @Override
                    public void onSuccess() {
                        logger.info("Message published successfully");
                    }

                    @Override
                    public void onError(ErrorInfo errorInfo) {
                        logger.error("Error publishing message: {}", errorInfo.message);
                    }
                });
                logger.info("Published message to channel {}", channel.name);
            } catch (AblyException e) {
                if (ably.options.queueMessages) {
                    logger.error("Failed to publish message", e);
                } else {
                    throw new RetriableException("Failed to publish to Ably when queueMessages is disabled.", e);
                }
            } catch (ChannelSinkConnectorConfig.ConfigException e) {
                logger.error(e.getMessage(), e);
                throw new ConnectException("Configuration error", e);
            }
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

        if (ably != null) {
            ably.close();
            ably = null;
        }
    }

    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }
}
