package com.ably.kafka.connect.utils;

import com.google.gson.Gson;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Objects;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class StructUtilsTest {
    private AvroToStruct avroToStruct;

    public StructUtilsTest() throws RestClientException, IOException {

    }

    @BeforeEach
    public void setup() {
        avroToStruct = new AvroToStruct();
    }

    @Test
    void testSimpleStructToJson() throws IOException, RestClientException {
        //given
        final AvroToStruct.Card card = new AvroToStruct.Card("cardId", 10000, "pocketId", "123");
        Struct struct = avroToStruct.getSimpleStruct(card);

        //when
        final String jsonString = StructUtils.toJsonString(struct);

        //then
        final AvroToStruct.Card receivedCard = new Gson().fromJson(jsonString, AvroToStruct.Card.class);
        assertEquals(receivedCard, card);
    }
}
