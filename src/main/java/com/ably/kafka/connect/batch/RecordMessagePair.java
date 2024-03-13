package com.ably.kafka.connect.batch;

import com.ably.kafka.connect.client.BatchSpec;
import io.ably.lib.types.Message;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Kafka Records with outgoing Ably Batch Spec.
 */
public class RecordMessagePair {
    private final SinkRecord kafkaRecord;
    private final Message message;
    private final String channelName;
    /**
     * Construct a new record-message pairing.
     */
    RecordMessagePair(SinkRecord kafkaRecord, Message message, String channelName) {
        this.kafkaRecord = kafkaRecord;
        this.message = message;
        this.channelName = channelName;
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
    public BatchSpec getBatchSpec() {
        return new BatchSpec(channelName, message);
    }

    /**
     * Returns Ably message associated with Kafka Record
     */
    public Message getMessage() {
        return message;
    }

    /**
     * Returns channel name
     */
    public String getChannelName() {
        return channelName;
    }
}
