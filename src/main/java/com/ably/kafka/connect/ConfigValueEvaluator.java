package com.ably.kafka.connect;

import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.charset.StandardCharsets;

public class ConfigValueEvaluator {

    public static final String KEY_TOKEN = "${key}";
    public static final String TOPIC_TOKEN = "${topic}";

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
         if (key == null && pattern.contains(KEY_TOKEN)) {
             throw new IllegalArgumentException("Key is null and pattern contains ${key}");
         }

         //topic cannot be null so we don't need to check for it
         final String keyString = new String(key, StandardCharsets.UTF_8);
         return pattern.replace(KEY_TOKEN, keyString).replace(TOPIC_TOKEN, record.topic());
    }
}
