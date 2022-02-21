package com.ably.kafka.connect;

import org.apache.kafka.connect.sink.SinkRecord;

public interface ChannelSinkChannelConfig {
    String channelName(SinkRecord record);
}
