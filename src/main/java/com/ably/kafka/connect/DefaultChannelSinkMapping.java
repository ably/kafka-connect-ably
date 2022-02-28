package com.ably.kafka.connect;

import io.ably.lib.realtime.AblyRealtime;
import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.channels.Channel;

public class DefaultChannelSinkMapping implements ChannelSinkMapping {
    @Override
    public Channel getChannel(SinkRecord sinkRecord, AblyRealtime ablyRealtime) {
        return null;
    }
}
