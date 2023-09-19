package com.ably.kafka.connect.utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.ably.lib.types.MessageExtras;
import io.ably.lib.util.JsonUtils;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Base64;
import java.util.Map;

public class RecordHeaderConversions {

    private static final Logger logger = LoggerFactory.getLogger(RecordHeaderConversions.class);
    private static final String PUSH_HEADER = "com.ably.extras.push";
    private static final String KAFKA_KEY = "kafka";
    private static final String HEADERS_KEY = "headers";
    private static final String PUSH_KEY = "push";
    /**
     * Returns the extras object to use when converting a Kafka message
     * to an Ably message.
     * <p>
     * If the Kafka message has a key, it is base64 encoded and set as the
     * "key" field in the extras and is added under "kafka" extras.
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
    public static MessageExtras toMessageExtras(final SinkRecord record) {
       final Extras.Builder extrasBuilder = new Extras.Builder();
        final Extras extras = extrasBuilder.key(record.key())
            .recordHeaders(record.headers())
            .build();

        return extras.toMessageExtras();
    }

    /*
    Wrapper class representing extras and is to be used to simplify building of extras object
    * */
    private static class Extras {

        private JsonUtils.JsonUtilsObject kafkaObject;
        private JsonUtils.JsonUtilsObject topObject;
        private Object pushExtrasValue;

        private JsonUtils.JsonUtilsObject headersObject = null;

        static class Builder {
            final Extras extras;
            Builder() {
                extras = new Extras();
            }

            Extras build() {
                return extras;
            }

            Extras.Builder key(Object key) {
                if (key != null) {
                    String keyString = null;
                    if (key instanceof byte[]) {
                        keyString = Base64.getEncoder().encodeToString((byte[]) key);
                    } else if (key instanceof String) {
                        keyString = (String) key;
                    }
                    if (keyString != null) {
                        kafkaExtras().add("key", keyString);
                        topExtrasObject().add(KAFKA_KEY, kafkaExtras());
                    }
                }

                return this;
            }

            Extras.Builder recordHeaders(Headers headers) {
                if (!headers.isEmpty()) {
                    buildFromHeaders(headers);
                }

                return this;
            }

            private void buildFromHeaders(Headers headers) {
                for (Header header : headers) {
                    if (header.key().equals(PUSH_HEADER)) {
                        extras.pushExtrasValue = stringifyHeaderValueIfPrimitive(header);
                    } else {
                        headersObject().add(header.key(), stringifyHeaderValueIfPrimitive(header));
                    }
                }

                if (extras.headersObject != null) {
                    kafkaExtras().add(HEADERS_KEY, extras.headersObject);
                }

                buildPushExtras();
            }

            private void buildPushExtras() {
                if (extras.pushExtrasValue != null) {
                    String pushExtrasJson = null;
                    if (extras.pushExtrasValue instanceof String) {
                        pushExtrasJson = (String) extras.pushExtrasValue;
                    } else if (extras.pushExtrasValue instanceof Map) {
                        pushExtrasJson = new Gson().toJson(extras.pushExtrasValue, Map.class);
                    }
                    if (pushExtrasJson != null) {
                        final JsonObject pushPayload = buildPushPayload(pushExtrasJson);
                        if (pushPayload != null) {
                            topExtrasObject().add(PUSH_KEY, pushPayload);
                        }
                    }
                }
            }

            /**
             * Validate and build push payload using given pushJson
             *
             * @param pushJson json string payload to validate and create a valid payload from
             */
            private JsonObject buildPushPayload(final String pushJson) {
                final Gson gson = new Gson();
                final Map<String, Object> map = gson.fromJson(pushJson, Map.class);
                if (map.get("notification") == null) {
                    logger.error("Push payload is invalid : No 'notification' field was found");
                    return null;
                }
                final Map notification = (Map) map.get("notification");
                if (notification.get("title") == null) {
                    logger.error("Push payload is invalid : No 'title' for notification was found");
                    return null;
                }

                if (notification.get("body") == null) {
                    logger.error("Push payload is invalid : No 'body' for notification was found");
                    return null;
                }
                return gson.toJsonTree(map).getAsJsonObject();
            }

            private JsonUtils.JsonUtilsObject kafkaExtras() {
                if (extras.kafkaObject == null) {
                    extras.kafkaObject = JsonUtils.object();
                }
                return extras.kafkaObject;
            }
            private JsonUtils.JsonUtilsObject topExtrasObject() {
                if (extras.topObject == null) {
                    extras.topObject = JsonUtils.object();
                }
                return extras.topObject;
            }

            private JsonUtils.JsonUtilsObject headersObject() {
                if (extras.headersObject == null) {
                    extras.headersObject = JsonUtils.object();
                }
                return extras.headersObject;
            }

        }

        @Nullable
        MessageExtras toMessageExtras(){
            if (topObject != null){
                return new MessageExtras(topObject.toJson());
            }
            return null;
        }

        /**
         * Stringify header value if it is primitive.
         */
        private static Object stringifyHeaderValueIfPrimitive(Header header) {
            if (header.schema() != null && header.schema().type().isPrimitive()) {
                return String.valueOf(header.value());
            } else {
                return header.value();
            }
        }

    }
}
