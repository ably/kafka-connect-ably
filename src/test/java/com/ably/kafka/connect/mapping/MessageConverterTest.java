package com.ably.kafka.connect.mapping;

import com.ably.kafka.connect.utils.MockedHeader;
import io.ably.lib.types.Message;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MessageConverterTest {

    @Test
    void propagatesEncodingFromHeadersToAblyMessage() {
        // given
        Map<String, String> headersMap = Map.of("com.ably.encoding", "json");
        List<Header> headers = MockedHeader.fromMap(headersMap);
        SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, null, Schema.BYTES_SCHEMA, "{\"foo\":\"bar\"}", 0, 0L, null, headers);
        // when
        Message message = MessageConverter.toAblyMessage(null, record);
        // then
        assertEquals(message.encoding, "json");
    }
}
