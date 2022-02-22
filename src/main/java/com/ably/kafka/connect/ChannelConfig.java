package com.ably.kafka.connect;

import org.apache.kafka.connect.sink.SinkRecord;

public interface ChannelConfig {
    String channelName(SinkRecord record);
}
