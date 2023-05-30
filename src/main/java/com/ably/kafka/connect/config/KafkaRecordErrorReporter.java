package com.ably.kafka.connect.config;

import org.apache.kafka.connect.sink.SinkRecord;

public interface KafkaRecordErrorReporter {
    void reportError(SinkRecord record, Exception e);
}
