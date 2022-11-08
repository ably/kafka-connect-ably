package com.ably.kafka.connect.transform;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;

class ByteArrayUtils {
    public static String utf8String(@Nullable byte[] value) {
        if (value == null){
            return null;
        }
        final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
        final ByteBuffer buf = ByteBuffer.wrap(value);
        try {
            final CharBuffer decoded = decoder.decode(buf);
            return decoded.toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }
}
