package com.ably.kafka.connect.utils;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;

public class ByteArrayUtils {
    public static boolean isUTF8Encoded(@Nonnull byte[] value) {
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
