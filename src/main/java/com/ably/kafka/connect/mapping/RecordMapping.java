package com.ably.kafka.connect.mapping;

import org.apache.kafka.connect.sink.SinkRecord;

public interface RecordMapping {
    /**
     * Map a SinkRecord to its corresponding String mapping, using an
     * injected template pattern for message or channel names.
     *
     * @param record The incoming SinkRecord instance
     * @return the corresponding channel or message name for this record
     *
     * @throws RecordMappingException if the mapping can't be performed, e.g.
     * due to missing field data.
     */
    String map(final SinkRecord record) throws RecordMappingException;
}
