package com.ably.kafka.connect.config;

import com.ably.kafka.connect.utils.ByteArrayUtils;
import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.charset.StandardCharsets;

public class ConfigValueEvaluator {
    /*
    Result class wrapping the evaluated value and whether it should be skipped or not
    This has been created to wrap the result of the evaluation of a config value when skippability is involved.
     * */
    public static class Result {
        private final boolean skip;
        private final String value;
        public Result(boolean skip, String value) {
            this.skip = skip;
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public boolean shouldSkip() {
            return skip;
        }
    }

    public static final String KEY_TOKEN = "#{key}";
    public static final String TOPIC_TOKEN = "#{topic}";

    /**
     * Converts a pattern to a value with the help of given record.
     * <p>
     * #{key} will be replaced with {@code record.key()} if {@code record.key()} is UTF-8 encoded.
     * #{topic} will be replaced with {@code record.topic()}
     *
     * @param record The SinkRecord to map
     * @param pattern The pattern to map
     * @param skippable Whether the pattern is skippable or not, if so, and the pattern has #{key} in it, and the key is not
     *                  set, the result will be marked as skipped. instead of throwing an exception.
     * @return Evaluated config value given the record and pattern
     */
    public Result evaluate(SinkRecord record, String pattern, boolean skippable) throws IllegalArgumentException{
        if (pattern == null) {
            return new Result(false, null);
        }

        final String keyString = getKeyString(record);

        if (keyString == null && pattern.contains(KEY_TOKEN)) {
            if (skippable) {
                return new Result(true, null);
            }
        }

        if (keyString != null) {
            return new Result(false, pattern.replace(KEY_TOKEN, keyString).replace(TOPIC_TOKEN, record.topic()));
        } else {
            return new Result(false, pattern.replace(TOPIC_TOKEN, record.topic()));
        }
    }

    /**
     * Function to retrieve key from SinkRecord
     * @param record
     * @return
     */
    private String getKeyString(SinkRecord record) {
        String keyString = null;
        final Object recordKey = record.key();
        if (recordKey != null) {
            if (recordKey instanceof byte[] && ByteArrayUtils.isUTF8Encoded((byte[]) recordKey)) {
                keyString = new String((byte[]) recordKey, StandardCharsets.UTF_8);
            } else if (recordKey instanceof String) {
                keyString = (String) recordKey;
            }
        }
        return keyString;
    }
}
