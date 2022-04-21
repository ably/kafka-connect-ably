package com.ably.kafka.connect.config;

import org.apache.kafka.connect.sink.SinkRecord;

public interface CipherConfig {
    String key(SinkRecord record);
}
