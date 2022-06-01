package com.ably.kafka.connect.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link StructToJsonConverter}.
 *
 * Unit tests in this class includes tests for the conversion of Connect structs to JSON.
 * Structs are created using the {@link AvroToStruct} class with different level of complexity of Avro schema.
 */
public class StructToJsonConverterTest {
    private AvroToStruct avroToStruct;
    private static final Gson gson = new GsonBuilder().serializeNulls().create();

    @BeforeEach
    public void setup() {
        avroToStruct = new AvroToStruct();
    }

    // Tests a struct with a simple flat Avro record with some primitive fields
    @Test
    void testSimpleStructToJson() throws IOException, RestClientException {
        // given
        final AvroToStruct.Card card = new AvroToStruct.Card("cardId", 10000, "pocketId", "123");
        Struct struct = avroToStruct.getStruct(card);

        // when
        final String jsonString = StructToJsonConverter.toJsonString(struct, gson);

        // then
        final AvroToStruct.Card receivedCard = new Gson().fromJson(jsonString, AvroToStruct.Card.class);
        assertEquals(receivedCard, card);
    }

    @Test
    void testComplexStructToJsonWithAllFieldsComplete() throws IOException, RestClientException {
        // given
        final AvroToStruct.Garage garage = exampleGarage("My garage");
        Struct struct = avroToStruct.getStruct(garage);

        // when
        final String jsonString = StructToJsonConverter.toJsonString(struct, gson);

        // then
        final AvroToStruct.Garage receivedGarage = new Gson().fromJson(jsonString, AvroToStruct.Garage.class);
        assertEquals(garage, receivedGarage);
    }

    @Test
    void testComplexStructToJsonWithNullStringValue() throws IOException, RestClientException {
        // given
        final AvroToStruct.Garage garage = exampleGarage(null);
        Struct struct = avroToStruct.getStruct(garage);

        // when
        final String jsonString = StructToJsonConverter.toJsonString(struct, gson);

        // then
        final AvroToStruct.Garage receivedGarage = new Gson().fromJson(jsonString, AvroToStruct.Garage.class);
        assertEquals(garage, receivedGarage);
    }

    @Test
    void testComplexStructToJsonWithNullMapValue() throws IOException, RestClientException {
        // given
        final AvroToStruct.Garage garage = exampleGarage("Something");
        garage.partMap = null;
        Struct struct = avroToStruct.getStruct(garage);

        // when
        final String jsonString = StructToJsonConverter.toJsonString(struct, gson);

        // then
        final AvroToStruct.Garage receivedGarage = new Gson().fromJson(jsonString, AvroToStruct.Garage.class);
        assertEquals(garage, receivedGarage);
    }

    @Test
    void testComplexStructToJsonWithNullArrayValue() throws IOException, RestClientException {
        // given
        final AvroToStruct.Garage garage = exampleGarage("My garage without cars");
        garage.cars = null;
        Struct struct = avroToStruct.getStruct(garage);

        // when
        final String jsonString = StructToJsonConverter.toJsonString(struct, gson);

        // then
        final AvroToStruct.Garage receivedGarage = new Gson().fromJson(jsonString, AvroToStruct.Garage.class);
        assertEquals(garage, receivedGarage);
    }

    @Test
    void testSimpleStructWithByteArrayThrowsException() throws IOException, RestClientException {
        // given
        final AvroToStruct.Computer computer = new AvroToStruct.Computer("My good computer", ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
        final Struct struct = avroToStruct.getStruct(computer);

        final Throwable exception = assertThrows(ConnectException.class, () -> StructToJsonConverter.toJsonString(struct, gson),
            "StructToJsonConverter.toJsonString(struct, gson) is expected to throw ConnectException");
        assertEquals(exception.getMessage(), "Bytes are currently not supported for conversion to JSON.");
    }

    // add tests with using a class containing general primitives
    @Test
    void testPrimitives() throws IOException, RestClientException {
        // given
        final AvroToStruct.Primitives primitivies = new AvroToStruct.Primitives(4343434L,
            4343,
            (short) 22,
            (byte) 100,
            4.3f,
            4.343443,
            false);
        final Struct struct = avroToStruct.getStruct(primitivies);

        // when
        final String jsonString = StructToJsonConverter.toJsonString(struct, gson);

        // then
        final AvroToStruct.Primitives receivedPrimitives = new Gson().fromJson(jsonString, AvroToStruct.Primitives.class);
        assertEquals(receivedPrimitives, primitivies);
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
