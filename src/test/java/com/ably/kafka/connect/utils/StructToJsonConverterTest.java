package com.ably.kafka.connect.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StructToJsonConverterTest {
    private AvroToStruct avroToStruct;
    private static final Gson gson = new GsonBuilder().serializeNulls().create();

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
        final String jsonString = StructToJsonConverter.toJsonString(struct, gson);

        //then
        final AvroToStruct.Card receivedCard = new Gson().fromJson(jsonString, AvroToStruct.Card.class);
        assertEquals(receivedCard, card);
    }

    @Test
    void testComplexStructToJson() throws IOException, RestClientException {
        //given
        final AvroToStruct.Garage garage = exampleGarage("My garage");
        Struct struct = avroToStruct.getComplexStruct(garage);

        //when
        final String jsonString = StructToJsonConverter.toJsonString(struct, gson);

        //then
        final AvroToStruct.Garage receivedGarage = new Gson().fromJson(jsonString, AvroToStruct.Garage.class);
        assertEquals(garage, receivedGarage);
    }

    @Test
    void testComplexStructToJsonWithNullStringValue() throws IOException, RestClientException {
        //given
        final AvroToStruct.Garage garage = exampleGarage(null);
        Struct struct = avroToStruct.getComplexStruct(garage);

        //when
        final String jsonString = StructToJsonConverter.toJsonString(struct,gson );

        //then
        final AvroToStruct.Garage receivedGarage = new Gson().fromJson(jsonString, AvroToStruct.Garage.class);
        assertEquals(garage, receivedGarage);
    }

    @Test
    void testComplexStructToJsonWithNullMapValue() throws IOException, RestClientException {
        //given
        final AvroToStruct.Garage garage = exampleGarage("Something");
        garage.partMap = null;
        Struct struct = avroToStruct.getComplexStruct(garage);

        //when
        final String jsonString = StructToJsonConverter.toJsonString(struct, gson);

        //then
        final AvroToStruct.Garage receivedGarage = new Gson().fromJson(jsonString, AvroToStruct.Garage.class);
        assertEquals(garage, receivedGarage);
    }

    @Test
    void testComplexStructToJsonWithNullArrayValue() throws IOException, RestClientException {
        //given
        final AvroToStruct.Garage garage = exampleGarage("My garage without cars");
        garage.cars = null;
        Struct struct = avroToStruct.getComplexStruct(garage);

        //when
        final String jsonString = StructToJsonConverter.toJsonString(struct,gson );

        //then
        final AvroToStruct.Garage receivedGarage = new Gson().fromJson(jsonString, AvroToStruct.Garage.class);
        assertEquals(garage, receivedGarage);
    }

    private AvroToStruct.Garage exampleGarage(String name) {
        final AvroToStruct.Part part = new AvroToStruct.Part("wheel", 100);
        final AvroToStruct.Part part2 = new AvroToStruct.Part("door", 200);
        final AvroToStruct.Part part3 = new AvroToStruct.Part("seat", 300);

        final AvroToStruct.Car car1 = new AvroToStruct.Car(new AvroToStruct.Engine(), List.of(part, part2, part3));
        final AvroToStruct.Car car2 = new AvroToStruct.Car(new AvroToStruct.Engine(), List.of(part, part2, part3));

        final Map<String, AvroToStruct.Part> partMap = Map.of("wheel", part, "door", part2, "seat", part3);

        return new AvroToStruct.Garage(name,List.of(car1, car2), partMap, AvroToStruct.Garage.GarageType.CAR, false);
    }
}
