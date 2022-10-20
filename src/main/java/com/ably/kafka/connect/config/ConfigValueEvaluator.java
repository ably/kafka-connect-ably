package com.ably.kafka.connect.config;

import com.ably.kafka.connect.utils.ByteArrayUtils;
import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.charset.StandardCharsets;

public class ConfigValueEvaluator {
    public static class Result{
        boolean skip;
        String value;
        public Result(boolean skip, String value) {
            this.skip = skip;
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public boolean isSkip() {
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
     * @param record    The SinkRecord to map
     * @param pattern   The pattern to map
     * @param skippable
     * @return Evaluated config value given the record and pattern
     */
    public Result evaluate(SinkRecord record, String pattern, boolean skippable) throws IllegalArgumentException{
        if (pattern == null) {
            return new Result(false,null);
        }
        final byte[] key = (byte[]) record.key();
        String keyString = null;

        //we only want to evalutate UTF-8 encoded strings
        if(key != null && ByteArrayUtils.isUTF8Encoded(key)) {
            keyString = new String(key, StandardCharsets.UTF_8);
        }
        if (keyString == null && pattern.contains(KEY_TOKEN)) {
            if (skippable){
                return new Result(true, null);
            }
            throw new IllegalArgumentException("Key is null or not a string type but pattern contains #{key}");
        }

        if (keyString != null) {
            return new Result(false,pattern.replace(KEY_TOKEN, keyString).replace(TOPIC_TOKEN, record.topic()));
        } else {
            return new Result(false, pattern.replace(TOPIC_TOKEN, record.topic()));
        }
    }
}
