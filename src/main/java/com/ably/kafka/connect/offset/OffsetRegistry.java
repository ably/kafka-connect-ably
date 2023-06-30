package com.ably.kafka.connect.offset;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.Map;

public interface OffsetRegistry {
    /**
     * Update the registry with offsets from the given records
     *
     * @param records records that have been successfully processed
     */
    void updateTopicPartitionToOffsetMap(List<SinkRecord> records);


    /**
     * Return a map of updated offsets for pre-commit. Offsets are returned from
     * the internal registry state.
     *
     * @param offsets The most recent offsets passed to pre-commit (and put)
     * @return A mapping of the most recently processed offsets per topic partition.
     */
    Map<TopicPartition, OffsetAndMetadata> updateOffsets(Map<TopicPartition, OffsetAndMetadata> offsets);
}
