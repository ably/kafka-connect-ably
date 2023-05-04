package com.ably.kafka.connect.client;

import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.config.ConfigValueEvaluator;
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

import java.util.concurrent.atomic.AtomicBoolean;

import static com.ably.kafka.connect.config.ChannelSinkConnectorConfig.CHANNEL_CONFIG;
import static com.ably.kafka.connect.config.ChannelSinkConnectorConfig.MESSAGE_CONFIG;
import static com.ably.kafka.connect.config.ChannelSinkConnectorConfig.SKIP_ON_KEY_ABSENCE;

public class DefaultAblyClient implements AblyClient {
    private static final Logger logger = LoggerFactory.getLogger(DefaultAblyClient.class);

    protected final ChannelSinkMapping channelSinkMapping;
    protected final MessageSinkMapping messageSinkMapping;
    protected final ChannelSinkConnectorConfig connectorConfig;

    private final ConfigValueEvaluator configValueEvaluator;
    private AblyRealtime realtime;

    //When this is true, the client should abort all publishing operations and throw an exception
    private final AtomicBoolean connectionFailed = new AtomicBoolean(false);

    public DefaultAblyClient(ChannelSinkConnectorConfig connectorConfig, ChannelSinkMapping channelSinkMapping,
                             MessageSinkMapping messageSinkMapping, ConfigValueEvaluator configValueEvaluator) {
        this.connectorConfig = connectorConfig;
        this.channelSinkMapping = channelSinkMapping;
        this.messageSinkMapping = messageSinkMapping;
        this.configValueEvaluator = configValueEvaluator;
    }

    @Override
    public void connect(SuspensionCallback suspensionCallback) throws ConnectException, AblyException {
        try {
            realtime = new AblyRealtime(connectorConfig.clientOptions);
            realtime.connection.on(connectionStateChange -> {
                if (connectionStateChange.current == ConnectionState.failed) {
                    logger.error("Connection failed with error: {}", connectionStateChange.reason);
                    connectionFailed.set(true);
                } else if (connectionStateChange.current == ConnectionState.connected) {
                    logger.info("Ably connection successfully established");
                    suspensionCallback.onSuspendedStateChange(false);
                } else if (connectionStateChange.current == ConnectionState.suspended) {
                    logger.info("Ably connection is suspended");
                    suspensionCallback.onSuspendedStateChange(true);
                }
            });

        } catch (AblyException e) {
            logger.error("error initializing ably client", e);
        }
    }

    @Override
    public void publishFrom(SinkRecord record) throws ConnectException {
        if (connectionFailed.get()) {
            //this exception should cause the calling task to abort
            throw new ConnectException("Cannot publish to Ably when connection failed");
        }

        if (shouldSkip(record)) return;

        try {
            final Channel channel = channelSinkMapping.getChannel(record, realtime);
            final Message message = messageSinkMapping.getMessage(record);

            channel.publish(message, new CompletionListener() {
                @Override
                public void onSuccess() {
                    logger.info("Published message to channel {} with id {}", channel.name, message.id);
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

    protected boolean shouldSkip(SinkRecord record) {
        final boolean skipOnKeyAbsence = connectorConfig.getBoolean(SKIP_ON_KEY_ABSENCE);

        if (skipOnKeyAbsence) {
            final String messageConfig = connectorConfig.getString(MESSAGE_CONFIG);
            final String channelConfig = connectorConfig.getString(CHANNEL_CONFIG);
            final ConfigValueEvaluator.Result messageResult = configValueEvaluator.evaluate(record, messageConfig, true);
            final ConfigValueEvaluator.Result channelResult = configValueEvaluator.evaluate(record, channelConfig, true);

            if (messageResult.shouldSkip() || channelResult.shouldSkip()) {
                logger.warn("Skipping record as record key is not available in a record where the config for either" +
                    " 'message.name' or 'channel' is configured to use #{key} as placeholders {}", record);
                return true;
            }
        }
        return false;
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
}
