package com.ably.kafka.connect.mapping;

import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.config.ConfigValueEvaluator;
import org.apache.kafka.connect.sink.SinkRecord;

import javax.annotation.Nonnull;

import static com.ably.kafka.connect.config.ChannelSinkConnectorConfig.CHANNEL_CONFIG;

public class DefaultChannelSinkMapping implements ChannelSinkMapping {

    private final ChannelSinkConnectorConfig connectorConfig;
    private final ConfigValueEvaluator configValueEvaluator;

    public DefaultChannelSinkMapping(ChannelSinkConnectorConfig connectorConfig, ConfigValueEvaluator configValueEvaluator) {
        this.connectorConfig = connectorConfig;
        this.configValueEvaluator = configValueEvaluator;
    }

    @Override
    public String getChannelName(@Nonnull SinkRecord sinkRecord) {
        final boolean skip = connectorConfig.getBoolean(ChannelSinkConnectorConfig.SKIP_ON_KEY_ABSENCE);
        final String channelNamePattern = connectorConfig.getString(CHANNEL_CONFIG);
        return configValueEvaluator.evaluate(sinkRecord, channelNamePattern, skip).getValue();
    }
}
