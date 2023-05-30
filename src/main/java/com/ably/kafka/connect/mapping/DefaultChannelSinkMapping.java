package com.ably.kafka.connect.mapping;

import com.ably.kafka.connect.config.ChannelConfig;
import com.ably.kafka.connect.config.ConfigValueEvaluator;
import org.apache.kafka.connect.sink.SinkRecord;

import javax.annotation.Nonnull;

public class DefaultChannelSinkMapping implements ChannelSinkMapping {
    private final ConfigValueEvaluator configValueEvaluator;
    private final ChannelConfig channelConfig;

    public DefaultChannelSinkMapping(ConfigValueEvaluator configValueEvaluator, ChannelConfig channelConfig) {
        this.configValueEvaluator = configValueEvaluator;
        this.channelConfig = channelConfig;
    }

    @Override
    public String getChannelName(@Nonnull SinkRecord sinkRecord) {
        return configValueEvaluator.evaluate(sinkRecord, channelConfig.getName(), false).getValue();
    }
}
