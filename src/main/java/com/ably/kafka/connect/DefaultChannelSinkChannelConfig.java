package com.ably.kafka.connect;


import org.apache.kafka.connect.sink.SinkRecord;

public class DefaultChannelSinkChannelConfig implements ChannelSinkChannelConfig {
    @Override
    public String channelName(SinkRecord record) {
        return record.topic();
    }
}
