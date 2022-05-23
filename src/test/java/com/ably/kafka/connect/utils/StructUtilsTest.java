package com.ably.kafka.connect.utils;

import com.google.gson.Gson;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StructUtilsTest {
    private AvroToStruct avroToStruct;

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

    @Test
    void testComplexStructToJson() throws IOException, RestClientException {
        //given
        final AvroToStruct.Garage garage = exampleGarage();
        Struct struct = avroToStruct.getComplexStruct(garage);

        //when
        final String jsonString = StructUtils.toJsonString(struct);

        //then
        final AvroToStruct.Garage receivedGarage = new Gson().fromJson(jsonString, AvroToStruct.Garage.class);
        assertEquals(garage, receivedGarage);
    }

    private AvroToStruct.Garage exampleGarage() {
        final AvroToStruct.Part part = new AvroToStruct.Part("wheel", 100);
        final AvroToStruct.Part part2 = new AvroToStruct.Part("door", 200);
        final AvroToStruct.Part part3 = new AvroToStruct.Part("seat", 300);

        final AvroToStruct.Car car1 = new AvroToStruct.Car(new AvroToStruct.Engine(), List.of(part, part2, part3));
        final AvroToStruct.Car car2 = new AvroToStruct.Car(new AvroToStruct.Engine(), List.of(part, part2, part3));

        final Map<String, AvroToStruct.Part> partMap = Map.of("wheel", part, "door", part2, "seat", part3);

        return new AvroToStruct.Garage(List.of(car1, car2), partMap, AvroToStruct.Garage.GarageType.CAR, false);
    }
}
