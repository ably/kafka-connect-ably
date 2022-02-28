package com.ably.kafka.connect;

import io.ably.lib.realtime.AblyRealtime;
import io.ably.lib.realtime.Channel;
import io.ably.lib.types.AblyException;
import org.apache.kafka.connect.sink.SinkRecord;

import javax.annotation.Nonnull;

public class DefaultChannelSinkMapping implements ChannelSinkMapping {
    private final ChannelSinkConnectorConfig sinkConnectorConfig;
    public DefaultChannelSinkMapping(@Nonnull ChannelSinkConnectorConfig config) {
        sinkConnectorConfig = config;
    }

    @Override
    public Channel getChannel(@Nonnull SinkRecord sinkRecord, @Nonnull AblyRealtime ablyRealtime) throws AblyException {
        return ablyRealtime.channels.get(sinkConnectorConfig.channelName, sinkConnectorConfig.channelOptions);
    }
}
