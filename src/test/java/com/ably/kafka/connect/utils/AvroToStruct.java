package com.ably.kafka.connect.utils;

import com.google.gson.Gson;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class AvroToStruct {

    Struct getSimpleStruct(final Card card) throws RestClientException, IOException {
        Properties defaultConfig = new Properties();
        defaultConfig.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");

        final SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
        final Schema schema = ReflectData.get().getSchema(Card.class);
        schemaRegistry.register("simple-schema", schema);

        final KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer(schemaRegistry, new HashMap(defaultConfig));
        final AvroConverter converter = new AvroConverter(schemaRegistry);
        converter.configure(Collections.singletonMap("schema.registry.url", "bogus"), false);

        final IndexedRecord cardRecord = createCardRecord(card, schema);

        final byte[] bytes = avroSerializer.serialize("DEFAULT_TOPIC", cardRecord);

        final SchemaAndValue schemaAndValue = converter.toConnectData("DEFAULT_TOPIC", bytes);
        return (Struct) schemaAndValue.value();
    }

    Struct getComplexStruct(final Garage garage) throws RestClientException, IOException {
        Properties defaultConfig = new Properties();
        defaultConfig.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");

        final SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
        final Schema schema = ReflectData.get().getSchema(Garage.class);
        schemaRegistry.register("complex-schema", schema);

        final KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer(schemaRegistry, new HashMap(defaultConfig));
        final AvroConverter converter = new AvroConverter(schemaRegistry);
        converter.configure(Collections.singletonMap("schema.registry.url", "bogus"), false);

        final IndexedRecord cardRecord = createComplexRecord(garage, schema);

        final byte[] bytes = avroSerializer.serialize("DEFAULT_TOPIC", cardRecord);

        final SchemaAndValue schemaAndValue = converter.toConnectData("DEFAULT_TOPIC", bytes);
        return (Struct) schemaAndValue.value();
    }

    private IndexedRecord createCardRecord(Card card, Schema schema) throws IOException {
        GenericRecord avroRecord = new GenericData.Record(schema);

        avroRecord.put("cardId", card.cardId);
        avroRecord.put("limit", card.limit);
        avroRecord.put("pocketId", card.pocketId);
        avroRecord.put("cvv", card.cvv);
        return avroRecord;
    }

    private IndexedRecord createComplexRecord(final Garage garage, Schema schema) throws IOException {
        final String json = new Gson().toJson(garage);
        return new JsonAvroConverter().convertToGenericDataRecord(json.getBytes(StandardCharsets.UTF_8), schema);
    }

    static class Card {
        private String cardId;
        private int limit;
        private String pocketId;
        private String cvv;

        public Card(String cardId, int limit, String pocketId, String cvv) {
            this.cardId = cardId;
            this.limit = limit;
            this.pocketId = pocketId;
            this.cvv = cvv;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Card)) return false;
            Card card = (Card) o;
            return limit == card.limit && cardId.equals(card.cardId) && pocketId.equals(card.pocketId) && cvv.equals(card.cvv);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cardId, limit, pocketId, cvv);
        }
    }

    static class Garage {
        Garage(List<Car> cars, Map<String, Part> partMap, GarageType type, boolean isOpen) {
            this.cars = cars;
            this.partMap = partMap;
            this.type = type;
            this.isOpen = isOpen;
        }

        enum GarageType {
            CAR, TRUCK
        }
        final List<Car> cars;
        final Map<String,Part> partMap;
        final GarageType type;
        final boolean isOpen;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Garage)) return false;
            Garage garage = (Garage) o;
            return isOpen == garage.isOpen && Objects.equals(cars, garage.cars) && Objects.equals(partMap, garage.partMap) && type == garage.type;
        }

        @Override
        public int hashCode() {
            return Objects.hash(cars, partMap, type, isOpen);
        }
    }

    static class Car {
        final Engine engine;
        final List<Part> parts;

        Car(Engine engine, List<Part> parts) {
            this.engine = engine;
            this.parts = parts;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Car)) return false;
            Car car = (Car) o;
            return Objects.equals(engine, car.engine) && Objects.equals(parts, car.parts);
        }

        @Override
        public int hashCode() {
            return Objects.hash(engine, parts);
        }
    }

    static class Engine {
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Engine) return true;
            return false;
        }
    }

    static class Part {
        final String name;
        final int price;

        Part(String name, int price) {
            this.name = name;
            this.price = price;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Part)) return false;
            Part part = (Part) o;
            return price == part.price && Objects.equals(name, part.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, price);
        }
    }
}