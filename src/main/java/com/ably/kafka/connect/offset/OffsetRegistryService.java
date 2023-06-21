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

    /**
     * Updates the topicPartitionToOffsetMap with the latest offset for each topic partition.
     * @param records
     */
    @Override
    public void updateTopicPartitionToOffsetMap(List<SinkRecord> records) {
        for (SinkRecord record : records) {
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());

            if (topicPartitionToOffsetMap.containsKey(topicPartition)) {
                topicPartitionToOffsetMap.compute(topicPartition, (k, v) -> {
                  if(v < record.kafkaOffset()) {
                      return record.kafkaOffset();
                  } else {
                      return v;
                  }
                });
            } else {
                topicPartitionToOffsetMap.put(topicPartition, record.kafkaOffset());
            }
        }
    }

    /**
     * Updates the offsets that is received from the Precommit() method in SinkTask.
     * The logic is to return the tracked offsets in topicPartitionToOffsetMap
     * which is updated after the messages are sent to Ably.
     * @param offsets
     * @return
     */
    public Map<TopicPartition, OffsetAndMetadata> updateOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {

        Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();

        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            Long offset = entry.getValue().offset();

            if (topicPartitionToOffsetMap.containsKey(topicPartition)) {
                committedOffsets.put(topicPartition, new OffsetAndMetadata(offset));
            }
        }

       return committedOffsets;
    }

    @VisibleForTesting
    Map<TopicPartition, Long> getTopicPartitionToOffsetMap() {
        return topicPartitionToOffsetMap;
    }
}
