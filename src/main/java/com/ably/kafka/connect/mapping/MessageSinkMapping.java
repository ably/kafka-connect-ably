package com.ably.kafka.connect.mapping;

import io.ably.lib.types.Message;
import org.apache.kafka.connect.sink.SinkRecord;

public interface MessageSinkMapping {
    Message getMessage(SinkRecord record);
}

