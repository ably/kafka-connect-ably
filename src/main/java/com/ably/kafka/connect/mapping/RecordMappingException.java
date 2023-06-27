package com.ably.kafka.connect.mapping;

/**
 * Exception for reporting problems extracting a String channel
 * or message name from a SinkRecord using a pattern. Can be used if
 * the pattern refers to missing fields in the record, for example.
 */
public class RecordMappingException extends RuntimeException {
    /**
     * Actions that can be taken in response to a failed mapping attempt
     */
    public enum Action {
        SKIP_RECORD,
        STOP_TASK
    }

    private final Action action;

    public RecordMappingException(String message, Action action) {
        super(message);
        this.action = action;
    }

    /**
     * Action that should be taken in response to this error.
     *
     * @return either SKIP_RECORD or STOP_TASK, as required.
     */
    public Action action() {
        return action;
    }
}
