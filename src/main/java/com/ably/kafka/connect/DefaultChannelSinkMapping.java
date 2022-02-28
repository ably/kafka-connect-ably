package com.ably.kafka.connect;

import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.channels.Channel;

public class DefaultChannelSinkMapping implements ChannelSinkMapping {
    @Override
    public Channel getChannel(SinkRecord sinkRecord) {
        return null;
    }
}
