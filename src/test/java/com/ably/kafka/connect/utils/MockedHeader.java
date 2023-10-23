package com.ably.kafka.connect.utils;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MockedHeader implements Header {

    private String key;
    private Object value;

    public MockedHeader(String key, Object value) {
        this.key = key;
        this.value = value;
    }

    public static List<Header> fromMap(Map<String, ?> headers) {
        return headers.entrySet().stream()
            .map(entry -> new MockedHeader(entry.getKey(), entry.getValue()))
            .collect(Collectors.toList());
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public Schema schema() {
        return null;
    }

    @Override
    public Object value() {
        return value;
    }

    @Override
    public Header with(Schema schema, Object value) {
        return null;
    }

    @Override
    public Header rename(String key) {
        return null;
    }
}
