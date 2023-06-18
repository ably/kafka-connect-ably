package com.ably.kafka.connect.offset;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;

public interface OffsetRegistry {
    void update(TopicPartition partition, long offset);
    long latestOffset(TopicPartition partition);
    void updateTopicPartitionToOffsetMap(List<SinkRecord> records);
}
