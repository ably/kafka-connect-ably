package com.ably.kafka.connect.client;

import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.mapping.ChannelSinkMapping;
import com.ably.kafka.connect.mapping.MessageSinkMapping;
import io.ably.lib.realtime.AblyRealtime;
import io.ably.lib.realtime.Channel;
import io.ably.lib.realtime.CompletionListener;
import io.ably.lib.realtime.ConnectionState;
import io.ably.lib.types.AblyException;
import io.ably.lib.types.ErrorInfo;
import io.ably.lib.types.Message;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class DefaultAblyClient implements AblyClient {
    private static final Logger logger = LoggerFactory.getLogger(DefaultAblyClient.class);

    private final ChannelSinkMapping channelSinkMapping;
    private final MessageSinkMapping messageSinkMapping;
    private final ChannelSinkConnectorConfig connectorConfig;
    private AblyRealtime realtime;

    public DefaultAblyClient(ChannelSinkConnectorConfig connectorConfig, ChannelSinkMapping channelSinkMapping, MessageSinkMapping messageSinkMapping) {
        this.connectorConfig = connectorConfig;
        this.channelSinkMapping = channelSinkMapping;
        this.messageSinkMapping = messageSinkMapping;
    }

    @Override
    public void connect() throws ConnectException {
        final CountDownLatch connectedSignal = new CountDownLatch(1);
        try {
            realtime = new AblyRealtime(connectorConfig.clientOptions);
            realtime.connection.on(connectionStateChange -> {
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
    public void publishFrom(SinkRecord record) throws ConnectException {
        try {
            final Channel channel = channelSinkMapping.getChannel(record, realtime);
            final Message message = messageSinkMapping.getMessage(record);

            channel.publish(message, new CompletionListener() {
                @Override
                public void onSuccess() {
                }

                @Override
                public void onError(ErrorInfo errorInfo) {
                    handleAblyException(AblyException.fromErrorInfo(errorInfo));
                }
            });
        } catch (AblyException e) {
            handleAblyException(e);
        } catch (ChannelSinkConnectorConfig.ConfigException e) {
            logger.error(e.getMessage(), e);
            throw new ConnectException("Configuration error", e);
        }
    }

    private void handleAblyException(AblyException e) {
        if (realtime.options.queueMessages) {
            logger.error("Failed to publish message", e);
        } else {
            throw new RetriableException("Failed to publish to Ably when queueMessages is disabled.", e);
        }
    }

    @Override
    public void stop() {
        if (realtime != null) {
            realtime.close();
            realtime = null;
        }
    }

    @Override
    public boolean isConnected() {
        return realtime.connection.state == ConnectionState.connected;
    }
}
