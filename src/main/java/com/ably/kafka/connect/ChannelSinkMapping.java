package com.ably.kafka.connect;

import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.channels.Channel;

public interface ChannelSinkMapping {
    /**
     * Returns the channel for the given sink record.
     *
     * @param sinkRecord The sink record.
     * @return The channel.
     */
    Channel getChannel(SinkRecord sinkRecord);
}
