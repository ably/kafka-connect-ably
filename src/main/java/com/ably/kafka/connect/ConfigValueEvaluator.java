package com.ably.kafka.connect;

import io.ably.lib.util.JsonUtils;
import org.apache.kafka.connect.sink.SinkRecord;

public class ConfigValueEvaluator {

    public static final String KEY_TOKEN = "${key}";
    public static final String TOPIC_TOKEN = "${topic}";

    /**
     * Converts a pattern to a value with the help of given record.
     * <p>
     * ${key} will be replaced with record.key()
     * ${topic} will be replaced with record.topic
     *
     * @param record  The SinkRecord to map
     * @param pattern The pattern to map
     * @return Evaluated config value given the record and pattern
     */
    public String evaluate(SinkRecord record, String pattern) {
        if (pattern == null) {
            return null;
        }

        final JsonUtils.JsonUtilsObject extras = KafkaExtrasExtractor.createKafkaExtras(record);

        final String key = extras.toJson().get("key") != null ? extras.toJson().get("key").getAsString() : null;
        if (key == null && pattern.contains(KEY_TOKEN)) {
            throw new IllegalArgumentException("Key is null and pattern contains ${key}");
        }
        //topic cannot be null so we don't need to check for it

        if (key != null) {
            return pattern.replace(KEY_TOKEN, key).replace(TOPIC_TOKEN, record.topic());
        } else {
            return pattern.replace(TOPIC_TOKEN, record.topic());
        }
    }
}
