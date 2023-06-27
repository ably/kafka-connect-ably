package com.ably.kafka.connect.offset;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.Map;

public interface OffsetRegistry {
    void updateTopicPartitionToOffsetMap(List<SinkRecord> records);
    Map<TopicPartition, OffsetAndMetadata> updateOffsets(Map<TopicPartition, OffsetAndMetadata> offsets);
}
