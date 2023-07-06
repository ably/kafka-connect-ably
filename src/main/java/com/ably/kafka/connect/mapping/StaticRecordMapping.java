package com.ably.kafka.connect.mapping;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * A RecordMapping implementation that always maps constants to a constant value.
 * Intended for use with non-templated channel or message name configurations.
 */
public class StaticRecordMapping implements RecordMapping {

    private final String mapping;

    /**
     * Construct record mapping that always maps to a constant value
     *
     * @param mapping the constant to map all records to, no-matter what
     *                they contain.
     */
    public StaticRecordMapping(String mapping) {
        this.mapping = mapping;
    }

    @Override
    public String map(SinkRecord record) throws RecordMappingException {
        return mapping;
    }
}
