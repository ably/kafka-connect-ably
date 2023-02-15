package com.ably.kafka.connect.utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.ably.lib.types.MessageExtras;
import io.ably.lib.util.JsonUtils;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

public class RecordHeaderConversions {

    private static final Logger logger = LoggerFactory.getLogger(RecordHeaderConversions.class);
    private static final String PUSH_HEADER = "com.ably.extras.push";
    private static final String KAFKA_KEY = "kafka";
    private static final String PUSH_KEY = "push";
    /**
     * Returns the extras object to use when converting a Kafka message
     * to an Ably message.
     * <p>
     * If the Kafka message has a key, it is base64 encoded and set as the
     * "key" field in the extras and is added under "kafka" extras
     * <p>
     * If the Kafka message has headers, they are set as the "headers" field
     * in the extras.
     *
     * If the Kafka message has "com.ably.extras.push" header this is set as "push" extras for Ably message.
     *
     * @param record The sink record representing the Kafka message
     * @return The Kafka message extras object
     */
    @Nullable
    public static MessageExtras toMessageExtras(SinkRecord record) {
        JsonUtils.JsonUtilsObject kafkaExtras = null;
        JsonUtils.JsonUtilsObject extrasObject = null;

        Object pushExtras = null;
        byte[] key = (byte[]) record.key();
        if (key != null) {
            kafkaExtras = JsonUtils.object();
            final String keyValue = Base64.getEncoder().encodeToString(key);
            kafkaExtras.add("key", keyValue);
        }

        if (record.headers().isEmpty()){
            if (kafkaExtras == null) {
                return null;
            }
            extrasObject = JsonUtils.object();
            extrasObject.add(KAFKA_KEY, kafkaExtras);
            return new MessageExtras(extrasObject.toJson());
        }

        JsonUtils.JsonUtilsObject headersObject = null;
        for (Header header : record.headers()) {
            if (header.key().equals(PUSH_HEADER)) {
                pushExtras = header.value();
            } else {
                if (kafkaExtras == null) {
                    kafkaExtras = JsonUtils.object();
                }
                if (headersObject == null) {
                    headersObject = JsonUtils.object();
                }
                headersObject.add(header.key(), header.value());
            }
        }

        if (kafkaExtras == null && pushExtras == null) {
            return null;
        }

        if (kafkaExtras != null && headersObject != null) {
            kafkaExtras.add("headers", headersObject);
        }

        if (kafkaExtras != null) {
            extrasObject = JsonUtils.object();
            extrasObject.add(KAFKA_KEY, kafkaExtras);
        }

        if (pushExtras != null) {
            String pushExtrasJson = null;
            if (pushExtras instanceof String){
                pushExtrasJson = (String) pushExtras;
            } else if (pushExtras instanceof LinkedHashMap) {
                // tests outputs this type when reading from a Java based producer
                pushExtrasJson = new Gson().toJson(pushExtras, Map.class);
            }
            if (pushExtrasJson != null){
                final JsonObject pushPayload = buildPushPayload(pushExtrasJson);
                if (pushPayload != null) {
                    if (extrasObject == null) {
                        extrasObject = JsonUtils.object();
                    }
                    extrasObject.add(PUSH_KEY, pushPayload);
                }
            }
        }

        if (extrasObject != null){
            return new MessageExtras(extrasObject.toJson());
        }

        return null;
    }
    private static class PushPayload {
        private Notification notification;
        static class Notification{
            private String title;
            private String body;
        }
        private Object data;
    }
    private static JsonObject buildPushPayload(final String pushJson){
        final Gson gson = new Gson();
        final PushPayload pushPayload = gson.fromJson(pushJson, PushPayload.class);

        if (pushPayload.notification == null){
            logger.error("Push payload is invalid : No 'notification' field was found");
            return null;
        }

        if (pushPayload.notification.title == null){
            logger.error("Push payload is invalid : No 'title' for notification was found");
            return null;
        }

        if (pushPayload.notification.body == null){
            logger.error("Push payload is invalid : No 'body' for notification was found");
            return null;
        }

        return gson.toJsonTree(pushPayload).getAsJsonObject();
    }
}
