package com.ably.kafka.connect.mapping;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * A RecordMapping implementation that always returns null.
 * Intended for use when message.name config has been deliberately unset, because
 * message names are not required in the output Ably Messages.
 * Not to be used with channel names, as null channel names are always an error.
 */
public class NullRecordMapping implements RecordMapping {
    @Override
    public String map(SinkRecord record) throws RecordMappingException {
        return null;
    }
}
