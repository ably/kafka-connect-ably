package com.ably.kafka.connect;

import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class ConfigValueEvaluator {
    /**
     * Converts a pattern to a value with the help of given record.
     *
     * ${key} will be replaced with record.key()
     * ${topic} will be replaced with record.topic

     * @param record The SinkRecord to map
     * @param pattern The pattern to map
     * @return The String representation of the SinkRecord
     */
     public String evaluate(SinkRecord record, String pattern) {
         final byte[] key = (byte[]) record.key();
         if (key == null) {
             throw new IllegalArgumentException("Key cannot be null");
         }
         if (record.topic() == null) {
             throw new IllegalArgumentException("Topic cannot be null");
         }

         final String keyString = new String(key, StandardCharsets.UTF_8);
         return pattern.replace("${key}", keyString).replace("${topic}", record.topic());
    }
}
