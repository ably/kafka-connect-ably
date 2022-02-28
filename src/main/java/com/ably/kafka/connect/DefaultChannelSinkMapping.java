package com.ably.kafka.connect;

import io.ably.lib.realtime.AblyRealtime;
import io.ably.lib.realtime.Channel;
import io.ably.lib.types.AblyException;
import org.apache.kafka.connect.sink.SinkRecord;

public class DefaultChannelSinkMapping implements ChannelSinkMapping {
    private final ChannelSinkConnectorConfig sinkConnectorConfig;
    public DefaultChannelSinkMapping(ChannelSinkConnectorConfig config) {
        sinkConnectorConfig = config;
    }

    @Override
    public Channel getChannel(SinkRecord sinkRecord, AblyRealtime ablyRealtime) throws AblyException {
        return ablyRealtime.channels.get(sinkConnectorConfig.channelName, sinkConnectorConfig.channelOptions);
    }
}
