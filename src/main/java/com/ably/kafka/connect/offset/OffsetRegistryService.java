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
        Map<TopicPartition, Long> maximumOffsets = getMaximumOffset(records);
        for (Map.Entry<TopicPartition, Long> entry : maximumOffsets.entrySet()) {
            topicPartitionToOffsetMap.compute(entry.getKey(), (k, v) -> {
                if (v == null || v < entry.getValue()) {
                    return entry.getValue();
                } else {
                    return v;
                }
            });
        }
    }

    /**
     * Function to calculate the maximum offset for each topic partition.
     * @param records List of Sink records
     * @return Map of TopicPartition to Offset
     */
    public Map<TopicPartition, Long> getMaximumOffset(List<SinkRecord> records) {
        HashMap<TopicPartition, Long> maxOffset = new HashMap<>();
        for(SinkRecord record: records) {

            TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
            if(maxOffset.containsKey(tp)) {
                if(maxOffset.get(tp) < record.kafkaOffset()) {
                    maxOffset.put(new TopicPartition(record.topic(), record.kafkaPartition()), record.kafkaOffset());
                }
            } else {
                maxOffset.put(new TopicPartition(record.topic(), record.kafkaPartition()), record.kafkaOffset());
            }

        }

        return maxOffset;
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

            if (topicPartitionToOffsetMap.containsKey(topicPartition)) {
                Long endOffset = topicPartitionToOffsetMap.get(topicPartition) + 1;
                committedOffsets.put(topicPartition, new OffsetAndMetadata(endOffset));
            }
        }

       return committedOffsets;
    }

    @VisibleForTesting
    Map<TopicPartition, Long> getTopicPartitionToOffsetMap() {
        return topicPartitionToOffsetMap;
    }
}
