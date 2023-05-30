package com.ably.kafka.connect.mapping;

import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import io.ably.lib.realtime.AblyRealtime;
import io.ably.lib.realtime.Channel;
import io.ably.lib.types.AblyException;
import org.apache.kafka.connect.sink.SinkRecord;

import javax.annotation.Nonnull;

public interface ChannelSinkMapping {
    /**
     * Create a new Ably channel based on the sink record.
     * @param sinkRecord
     * @return
     * @throws AblyException
     */
    String getChannelName(@Nonnull SinkRecord sinkRecord);
}
