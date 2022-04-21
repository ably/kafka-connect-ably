package com.ably.kafka.connect.mapping;

import com.ably.kafka.connect.config.ChannelConfig;
import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.config.ConfigValueEvaluator;
import io.ably.lib.realtime.AblyRealtime;
import io.ably.lib.realtime.Channel;
import io.ably.lib.types.AblyException;
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
    public Channel getChannel(@Nonnull SinkRecord sinkRecord, @Nonnull AblyRealtime ablyRealtime) throws AblyException,
            ChannelSinkConnectorConfig.ConfigException {
        final String channelName = configValueEvaluator.evaluate(sinkRecord, channelConfig.getName());
        return ablyRealtime.channels.get(channelName, channelConfig.getOptions(sinkRecord));
    }
}
