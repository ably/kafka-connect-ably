package com.ably.kafka.connect;

import com.ably.kafka.connect.mapping.MessageConverter;
import com.ably.kafka.connect.utils.AvroToStruct;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.ably.lib.types.MessageExtras;
import io.ably.lib.util.JsonUtils;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MessageConversionTest {

    private final AvroToStruct avroToStruct = new AvroToStruct();

    @Test
    void testGetMessage_messageDataIsTheSameWithRecordValue() {
        //given
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, "key".getBytes(), Schema.BYTES_SCHEMA, "value", 0);

        //when
        final Object messageData = MessageConverter.toAblyMessage("name", record).data;

        //then
        assertEquals(record.value(), messageData);
    }


    @Test
    @Disabled
    // TODO: Message idempotency is not currently supported in Ably Batch API submissions
    void testGetMessage_messageIdIsSetBasedOnRecordValues() {
        //given
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, "key".getBytes(), Schema.BYTES_SCHEMA, "value", 0);

        //when
        final String messageId = MessageConverter.toAblyMessage("name", record).id;

        //then
        assertEquals(messageId, String.format("%d:%d:%d", record.topic().hashCode(), record.kafkaPartition(), record.kafkaOffset()));
    }

    @Test
    void testGetMessage_sentAndReceivedExtrasKeysAreTheSame() {
        //given
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, "key".getBytes(), Schema.BYTES_SCHEMA, "value", 0);

        //when
        final MessageExtras messageExtras = MessageConverter.toAblyMessage("name", record).extras;
        JsonUtils.JsonUtilsObject extras = JsonUtils.object();
        byte[] key = (byte[]) record.key();
        extras.add("key", Base64.getEncoder().encodeToString(key));

        final JsonObject receivedObject = messageExtras.asJsonObject().get("kafka").getAsJsonObject();

        String receivedKey = receivedObject.get("key").getAsString();
        String sentKey = Base64.getEncoder().encodeToString(key);

        //then
        assertEquals(receivedKey, sentKey);
    }

    @Test
    void testGetMessage_recordHeadersAreReceivedCorrectly() {
        //given
        final List<Header> headersList = new ArrayList<>();
        final Map<String, String> headersMap = Map.of("key1", "value1", "key2", "value2");
        for (Map.Entry<String, String> entry : headersMap.entrySet()) {
            headersList.add(new Header() {
                @Override
                public String key() {
                    return entry.getKey();
                }

                @Override
                public Schema schema() {
                    return null;
                }

                @Override
                public Object value() {
                    return entry.getValue();
                }

                @Override
                public Header with(Schema schema, Object value) {
                    return null;
                }

                @Override
                public Header rename(String key) {
                    return null;
                }
            });
        }
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, "key".getBytes(), Schema.BYTES_SCHEMA, "value", 0, 0L, null, headersList);

        //when
        final MessageExtras messageExtras = MessageConverter.toAblyMessage("name", record).extras;

        //then
        final JsonObject receivedObject = messageExtras.asJsonObject().get("kafka").getAsJsonObject();
        final JsonObject receivedHeaders = receivedObject.get("headers").getAsJsonObject();
        assertEquals(receivedHeaders.get("key1").getAsString(), "value1");
        assertEquals(receivedHeaders.get("key2").getAsString(), "value2");
    }

    @Test
    void testGetMessage_pushPayloadReceivedCorrectly() throws IOException {
        //given
        final URL url = MessageConversionTest.class.getResource("/example_push_payload.json");
        final String pushHeaderValue =  IOUtils.toString(url, StandardCharsets.UTF_8);

        final JsonElement expected = JsonParser.parseString(pushHeaderValue);

        final List<Header> headersList = new ArrayList<>();
        final Map<String, String> headersMap = Map.of("com.ably.extras.push", pushHeaderValue);
        for (final Map.Entry<String, String> entry : headersMap.entrySet()) {
            headersList.add(new Header() {
                @Override
                public String key() {
                    return entry.getKey();
                }

                @Override
                public Schema schema() {
                    return null;
                }

                @Override
                public Object value() {
                    return entry.getValue();
                }

                @Override
                public Header with(Schema schema, Object value) {
                    return null;
                }

                @Override
                public Header rename(String key) {
                    return null;
                }
            });
        }
        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, "key".getBytes(), Schema.BYTES_SCHEMA, "value", 0, 0L, null, headersList);

        //when
        final MessageExtras messageExtras = MessageConverter.toAblyMessage("name", record).extras;

        //then
        final JsonObject pushObject = messageExtras.asJsonObject().get("push").getAsJsonObject();
        assertEquals(expected, pushObject);
    }

    @Test
    void testMessageWithStructReceivedCorrectly() throws RestClientException, IOException {
        //given
        final AvroToStruct.Garage garage = exampleGarage("my garage");
        final Struct struct = avroToStruct.getStruct(garage);
        final Schema valueSchema = avroToStruct.getConnectSchema(garage);

        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, "key".getBytes(), valueSchema, struct, 0);

        //when
        final Object messageData = MessageConverter.toAblyMessage("name", record).data;

        //then
        assertTrue(messageData instanceof String);

        final String messageJson = (String) messageData;
        final AvroToStruct.Garage receivedGarage = new GsonBuilder().serializeNulls().create().fromJson(messageJson, AvroToStruct.Garage.class);
        assertEquals(garage, receivedGarage);
    }

    @Test
    void testThatExceptionIsThrownForNonStructSchemas() {
        //given
        final Schema mapSchema = SchemaBuilder.type(Schema.Type.MAP).build();
        final Map<String, String> map = Map.of("key1", "value1", "key2", "value2");

        final SinkRecord record = new SinkRecord("sink", 0, Schema.BYTES_SCHEMA, "key".getBytes(), mapSchema, map, 0);


        final Throwable exception = assertThrows(ConnectException.class, () -> MessageConverter.toAblyMessage("name", record),
            "sinkMapping.getMessage(record) is supposed to throw an exception for non-struct schemas");
        assertEquals(exception.getMessage(), String.format("Unsupported value schema type: %s", mapSchema.type()));
    }

    private AvroToStruct.Garage exampleGarage(String name) {
        final AvroToStruct.Part part = new AvroToStruct.Part("wheel", 100);
        final AvroToStruct.Part part2 = new AvroToStruct.Part("door", 200);
        final AvroToStruct.Part part3 = new AvroToStruct.Part("seat", 300);

        final AvroToStruct.Car car1 = new AvroToStruct.Car(new AvroToStruct.Engine(), List.of(part, part2, part3));
        final AvroToStruct.Car car2 = new AvroToStruct.Car(new AvroToStruct.Engine(), List.of(part, part2, part3));

        final Map<String, AvroToStruct.Part> partMap = Map.of("wheel", part, "door", part2, "seat", part3);

        return new AvroToStruct.Garage(name, List.of(car1, car2), partMap, AvroToStruct.Garage.GarageType.CAR, false);
    }
}
