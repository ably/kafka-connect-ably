package com.ably.kafka.connect;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;

public class ByteArrayUtils {
    public static boolean isUTF8Encoded(byte[] value) {
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
        ByteBuffer buf = ByteBuffer.wrap(value);
        try {
            decoder.decode(buf);
        } catch (CharacterCodingException e) {
            return false;
        }

        return true;
    }
}
