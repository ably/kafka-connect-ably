package com.ably.kafka.connect.utils;

import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;

import java.util.Arrays;
import java.util.Optional;

public enum SpecialAblyHeader {
    PUSH_HEADER("com.ably.extras.push"),
    ENCODING_HEADER("com.ably.encoding");

    private final String headerName;

    SpecialAblyHeader(String headerName) {
        this.headerName = headerName;
    }

    public static boolean isSpecialAblyHeader(Header header) {
        return Arrays.stream(SpecialAblyHeader.values()).anyMatch(ablyHeader -> ablyHeader.headerName.equals(header.key()));
    }

    public Optional<Header> tryFindInHeaders(Headers headers) {
        return Optional.ofNullable(headers.lastWithName(this.headerName));
    }
}
