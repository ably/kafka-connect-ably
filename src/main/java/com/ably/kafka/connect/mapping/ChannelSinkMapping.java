package com.ably.kafka.connect.mapping;

import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import io.ably.lib.realtime.AblyRealtime;
import io.ably.lib.realtime.Channel;
import io.ably.lib.types.AblyException;
import org.apache.kafka.connect.sink.SinkRecord;

import javax.annotation.Nonnull;

public interface ChannelSinkMapping {
    /**
     * Returns the channel for the given sink record.
     *
     * @param sinkRecord The sink record.
     * @param ablyRealtime AblyRealtime instance.
     * @return The channel.
     */
    Channel getChannel(@Nonnull SinkRecord sinkRecord,@Nonnull AblyRealtime ablyRealtime) throws AblyException,
            ChannelSinkConnectorConfig.ConfigException;

    String getChannelName(@Nonnull SinkRecord sinkRecord);
}
