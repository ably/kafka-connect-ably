package com.ably.kafka.connect.offset;

import com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is responsible for keeping track of the latest offset for each topic partition.
 */
public class OffsetRegistryService implements OffsetRegistry {

    // Hashmap of TopicPartition to Offsets.
    private final Map<TopicPartition, Long> topicPartitionToOffsetMap = new ConcurrentHashMap<>();

    public OffsetRegistryService() {

    }
    @Override
    public void update(TopicPartition partition, long offset) {

    }

    /**
     * Returns the latest offset for a given topic partition.
     * @param partition
     * @return Offset
     */
    @Override
    public long latestOffset(TopicPartition partition) {
        return topicPartitionToOffsetMap.get(partition);
    }

    /**
     * Updates the topicPartitionToOffsetMap with the latest offset for each topic partition.
     * @param records
     */
    @Override
    public void updateTopicPartitionToOffsetMap(List<SinkRecord> records) {
        for (SinkRecord record : records) {
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());

            if (topicPartitionToOffsetMap.containsKey(topicPartition)) {
                if (topicPartitionToOffsetMap.get(topicPartition) < record.kafkaOffset()) {
                    topicPartitionToOffsetMap.put(topicPartition, record.kafkaOffset());
                }
            } else {
                topicPartitionToOffsetMap.put(topicPartition, record.kafkaOffset());
            }
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> updateOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {

        Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();

        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            Long offset = entry.getValue().offset();

            if (topicPartitionToOffsetMap.containsKey(topicPartition)) {
                if (topicPartitionToOffsetMap.get(topicPartition) != 0) {
                    committedOffsets.put(topicPartition, new OffsetAndMetadata(offset));
                }
            }
        }

       return committedOffsets;
    }

    @VisibleForTesting
    Map<TopicPartition, Long> getTopicPartitionToOffsetMap() {
        return topicPartitionToOffsetMap;
    }
}
