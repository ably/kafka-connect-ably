package com.ably.kafka.connect;

import io.ably.lib.realtime.AblyRealtime;
import io.ably.lib.realtime.Channel;
import io.ably.lib.types.AblyException;
import org.apache.kafka.connect.sink.SinkRecord;

public interface ChannelSinkMapping {
    /**
     * Returns the channel for the given sink record.
     *
     * @param sinkRecord The sink record.
     * @param ablyRealtime AblyRealtime instance.
     * @return The channel.
     */
    Channel getChannel(SinkRecord sinkRecord, AblyRealtime ablyRealtime) throws AblyException;
}
