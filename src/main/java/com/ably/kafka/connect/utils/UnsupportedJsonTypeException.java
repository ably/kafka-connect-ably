package com.ably.kafka.connect.utils;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

public class UnsupportedJsonTypeException extends ConnectException {
    private final Schema.Type type;
    public UnsupportedJsonTypeException(Schema.Type type) {
        super(String.format("Type %s is not supported for JSON conversion", type.getName()));
        this.type = type;
    }

}
