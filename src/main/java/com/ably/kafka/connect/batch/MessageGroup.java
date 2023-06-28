package com.ably.kafka.connect.batch;

import com.ably.kafka.connect.client.BatchSpec;
import io.ably.lib.types.Message;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A grouping of Kafka Records by their outgoing Ably channel.
 */
public class MessageGroup {

    /**
     * Pairing of SinkRecord to corresponding Ably Message.
     * This is needed because we need to recover SinkRecord instances if Ably rejects a message.
     */
    static class RecordMessagePair {
        public final SinkRecord kafkaRecord;
        public final Message ablyMessage;
        /**
         * Construct a new record-message pairing.
         */
        RecordMessagePair(SinkRecord kafkaRecord, Message ablyMessage) {
            this.kafkaRecord = kafkaRecord;
            this.ablyMessage = ablyMessage;
        }

        /**
         * Returns the incoming Kafka SinkRecord
         */
        public SinkRecord getKafkaRecord() {
            return kafkaRecord;
        }

        /**
         * Returns the outgoing Ably Message
         */
        public Message getAblyMessage() {
            return ablyMessage;
        }

    }

    private final Map<String, List<RecordMessagePair>> groupedRecords;

    /**
     * Grouping of records ready for submission using Ably batch client.
     * Construct using MessageGrouper.group()
     */
    MessageGroup(Map<String, List<RecordMessagePair>> groupedRecords) {
        this.groupedRecords = groupedRecords;
    }

    /**
     * Generate a list of Ably BatchSpecs ready for publishing
     * @return List of BatchSpec instances that can be passed to AblyRest client.
     */
    public List<BatchSpec> specs() {
        return groupedRecords.entrySet().stream()
            .map(entry -> new BatchSpec(Set.of(entry.getKey()),
                entry.getValue().stream()
                    .map(RecordMessagePair::getAblyMessage)
                    .collect(Collectors.toList())))
            .collect(Collectors.toList());
    }

    /**
     * In the event of an entire batch submission failing, we may need to DLQ all
     * records in all BatchSpecs. This will return all SinkRecords in the corresponding
     * call to specs().
     *
     * @return all SinkRecords submitted in the BatchSpecs returned by specs()
     */
    public List<SinkRecord> allRecords() {
        return groupedRecords.values().stream()
            .flatMap(recs -> recs.stream()
                .map(RecordMessagePair::getKafkaRecord))
            .collect(Collectors.toList());
    }


    /**
     * Return all SinkRecords that correspond to a particular channel
     */
    public List<SinkRecord> recordsForChannel(final String channelName) {
        return groupedRecords.getOrDefault(channelName, Collections.emptyList()).stream()
            .map(RecordMessagePair::getKafkaRecord)
            .collect(Collectors.toList());
    }
}
