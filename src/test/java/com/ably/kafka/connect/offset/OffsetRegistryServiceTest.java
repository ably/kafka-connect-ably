package com.ably.kafka.connect.offset;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OffsetRegistryServiceTest {

    @Test
    public void testupdateTopicPartitionToOffsetMapRecordsInSameTopic() {
        OffsetRegistryService offsetRegistryService = new OffsetRegistryService();

        List<SinkRecord> records = List.of(
                // Partition 0
                new SinkRecord("topic1", 0, null, null, null, null, 3),

                // Partition 1
                new SinkRecord("topic1", 1, null, null, null, null, 2),
                new SinkRecord("topic1", 1, null, null, null, null, 1),
                new SinkRecord("topic1", 1, null, null, null, null, 0),

                // Partition 0
                new SinkRecord("topic1", 0, null, null, null, null, 0)
        );

        offsetRegistryService.updateTopicPartitionToOffsetMap(records);

        Map<TopicPartition, Long> expectedResult = new HashMap<>();
        expectedResult.put(new TopicPartition("topic1", 0), 3L);
        expectedResult.put(new TopicPartition("topic1", 1), 2L);

        assertEquals(offsetRegistryService.getTopicPartitionToOffsetMap(), expectedResult);
    }

    @Test
    public void testupdateTopicPartitionToOffsetMapRecordsInMultipleTopics() {
        OffsetRegistryService offsetRegistryService = new OffsetRegistryService();

        List<SinkRecord> records = List.of(
                // Partition 0
                new SinkRecord("topic1", 0, null, null, null, null, 3),

                // Partition 1
                new SinkRecord("topic1", 1, null, null, null, null, 2),
                new SinkRecord("topic1", 1, null, null, null, null, 1),
                new SinkRecord("topic1", 1, null, null, null, null, 0),

                // Partition 0
                new SinkRecord("topic1", 0, null, null, null, null, 0),

                // Topic 2 - Partition 0
                new SinkRecord("topic2", 0, null, null, null, null, 44444),
                new SinkRecord("topic2", 0, null, null, null, null, 1),

                // Topic 2 - Partition 4
                new SinkRecord("topic2", 4, null, null, null, null, 22222),
                new SinkRecord("topic2", 4, null, null, null, null, 1),
                new SinkRecord("topic2", 4, null, null, null, null, 22223)
        );

        offsetRegistryService.updateTopicPartitionToOffsetMap(records);

        Map<TopicPartition, Long> expectedResult = new HashMap<>();
        expectedResult.put(new TopicPartition("topic1", 0), 3L);
        expectedResult.put(new TopicPartition("topic1", 1), 2L);
        expectedResult.put(new TopicPartition("topic2", 0), 44444L);
        expectedResult.put(new TopicPartition("topic2", 4), 22223L);

        assertEquals(offsetRegistryService.getTopicPartitionToOffsetMap(), expectedResult);
    }

    @Test
    public void testUpdateOffsets() {
        OffsetRegistryService offsetRegistryService = new OffsetRegistryService();

        // There will be offsets of topics
        // that the sink task will not be subscribed to.
        Map<TopicPartition, OffsetAndMetadata> preCommitOffsets = new HashMap<>();
        preCommitOffsets.put(new TopicPartition("topic2", 2323), new OffsetAndMetadata(2200L));
        preCommitOffsets.put(new TopicPartition("topic2", 2323), new OffsetAndMetadata(1L));
        preCommitOffsets.put(new TopicPartition("topic2", 2323), new OffsetAndMetadata(2L));

        // Topics that the sink task is subscribed to.
        preCommitOffsets.put(new TopicPartition("topic1", 0), new OffsetAndMetadata(1L));
        preCommitOffsets.put(new TopicPartition("topic1", 1), new OffsetAndMetadata(44L));

        List<SinkRecord> records = List.of(
                // Partition 0
                new SinkRecord("topic1", 0, null, null, null, null, 3),

                // Partition 1
                new SinkRecord("topic1", 1, null, null, null, null, 2),
                new SinkRecord("topic1", 1, null, null, null, null, 1),
                new SinkRecord("topic1", 1, null, null, null, null, 0)
                );

        offsetRegistryService.updateTopicPartitionToOffsetMap(records);

        Map<TopicPartition, OffsetAndMetadata> result = offsetRegistryService.updateOffsets(preCommitOffsets);

        Map<TopicPartition, OffsetAndMetadata> expectedResult = new HashMap<>();
        expectedResult.put(new TopicPartition("topic1", 0), new OffsetAndMetadata(4L));
        expectedResult.put(new TopicPartition("topic1", 1), new OffsetAndMetadata(3L));

        assertEquals(result, expectedResult);
    }

    @Test
    public void testGetMaximumOffset() {
        OffsetRegistryService offsetRegistryService = new OffsetRegistryService();
        List<SinkRecord> records = List.of(
                // Partition 0
                new SinkRecord("topic1", 0, null, null, null, null, 3),

                // Partition 1
                new SinkRecord("topic1", 1, null, null, null, null, 2),
                new SinkRecord("topic1", 1, null, null, null, null, 1),
                new SinkRecord("topic1", 1, null, null, null, null, 0),

                // Topic 2 - Partition 5
                new SinkRecord("topic2", 5, null, null, null, null, 232323),
                new SinkRecord("topic2", 5, null, null, null, null, 4),
                new SinkRecord("topic2", 5, null, null, null, null, 4444444)
        );
        Map<TopicPartition, Long> maximumOffsetsMap = offsetRegistryService.getMaximumOffset(records);

        Map<TopicPartition, Long> expectedResult = new HashMap<>();
        expectedResult.put(new TopicPartition("topic1", 0), 3L);
        expectedResult.put(new TopicPartition("topic1", 1), 2L);

        expectedResult.put(new TopicPartition("topic2", 5), 4444444L);
        assertEquals(maximumOffsetsMap, expectedResult);
    }
}
