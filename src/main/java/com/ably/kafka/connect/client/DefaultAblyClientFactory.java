package com.ably.kafka.connect.client;

import com.ably.kafka.connect.config.ChannelConfig;
import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.config.ConfigValueEvaluator;
import com.ably.kafka.connect.config.DefaultChannelConfig;
import com.ably.kafka.connect.mapping.ChannelSinkMapping;
import com.ably.kafka.connect.mapping.DefaultChannelSinkMapping;
import com.ably.kafka.connect.mapping.DefaultMessageSinkMapping;
import com.ably.kafka.connect.mapping.MessageSinkMapping;
import com.ably.kafka.connect.utils.ClientOptionsLogHandler;
import io.ably.lib.types.AblyException;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DefaultAblyClientFactory implements AblyClientFactory {
    private static final Logger logger = LoggerFactory.getLogger(DefaultAblyClientFactory.class);

    @Override
    public DefaultAblyBatchClient create(Map<String, String> settings) throws AblyException {
        final ChannelSinkConnectorConfig connectorConfig = new ChannelSinkConnectorConfig(settings);
        final ConfigValueEvaluator configValueEvaluator = new ConfigValueEvaluator();
        final ChannelConfig channelConfig = new DefaultChannelConfig(connectorConfig);
        final ChannelSinkMapping channelSinkMapping = new DefaultChannelSinkMapping(configValueEvaluator, channelConfig);
        final MessageSinkMapping messageSinkMapping = new DefaultMessageSinkMapping(connectorConfig, configValueEvaluator);
        if (connectorConfig.clientOptions == null) {
            throw new ConfigException("Ably client options were not initialized due to invalid configuration.");
        }

        connectorConfig.clientOptions.logHandler = new ClientOptionsLogHandler(logger);
        //return new DefaultAblyClient(connectorConfig, channelSinkMapping, messageSinkMapping, configValueEvaluator);
        return new DefaultAblyBatchClient(connectorConfig, channelSinkMapping, messageSinkMapping, configValueEvaluator);

    }
}
