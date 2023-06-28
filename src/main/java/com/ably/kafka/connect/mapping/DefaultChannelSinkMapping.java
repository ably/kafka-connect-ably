package com.ably.kafka.connect.mapping;

import com.ably.kafka.connect.config.ChannelConfig;
import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.config.ConfigValueEvaluator;
import com.ably.kafka.connect.config.DefaultChannelConfig;
import org.apache.kafka.connect.sink.SinkRecord;

import javax.annotation.Nonnull;

public class DefaultChannelSinkMapping implements ChannelSinkMapping {

    private final ChannelSinkConnectorConfig connectorConfig;
    private final ConfigValueEvaluator configValueEvaluator;
    private final ChannelConfig channelConfig;

    public DefaultChannelSinkMapping(ChannelSinkConnectorConfig connectorConfig, ConfigValueEvaluator configValueEvaluator) {
        this.connectorConfig = connectorConfig;
        this.configValueEvaluator = configValueEvaluator;
        this.channelConfig = new DefaultChannelConfig(connectorConfig);
    }

    @Override
    public String getChannelName(@Nonnull SinkRecord sinkRecord) {
        final boolean skip = connectorConfig.getBoolean(ChannelSinkConnectorConfig.SKIP_ON_KEY_ABSENCE);
        return configValueEvaluator.evaluate(sinkRecord, channelConfig.getName(), skip).getValue();
    }
}
