package com.ably.kafka.connect;


import org.apache.kafka.connect.sink.SinkRecord;

public class DefaultChannelConfig implements ChannelConfig {
    @Override
    public String channelName(SinkRecord record) {
        return record.topic();
    }
}
