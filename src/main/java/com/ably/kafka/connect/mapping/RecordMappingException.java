package com.ably.kafka.connect.mapping;

/**
 * Exception for reporting problems extracting a String channel
 * or message name from a SinkRecord using a pattern. Can be used if
 * the pattern refers to missing fields in the record, for example.
 */
public class RecordMappingException extends RuntimeException {
    public RecordMappingException(String message) {
        super(message);
    }
}
