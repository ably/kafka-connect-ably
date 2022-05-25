package com.ably.kafka.connect.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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
import org.jetbrains.annotations.Nullable;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class AvroToStruct {

    Struct getStruct(final Object object) throws RestClientException, IOException {
        Properties defaultConfig = new Properties();
        defaultConfig.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");

        final SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
        Schema schema = getSchema(object);
        schemaRegistry.register("simple-schema", schema);

        final KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer(schemaRegistry, new HashMap(defaultConfig));
        final AvroConverter converter = new AvroConverter(schemaRegistry);
        converter.configure(Collections.singletonMap("schema.registry.url", "bogus"), false);
        IndexedRecord record = getIndexedRecord(object, schema);

        final byte[] bytes = avroSerializer.serialize("DEFAULT_TOPIC", record);

        final SchemaAndValue schemaAndValue = converter.toConnectData("DEFAULT_TOPIC", bytes);
        return (Struct) schemaAndValue.value();
    }

    @Nullable
    private IndexedRecord getIndexedRecord(Object object, Schema schema) throws IOException {
        if (object instanceof Card) {
            return createCardRecord((Card) object, schema);
        } else if (object instanceof Computer) {
            return createComputerRecord((Computer) object, schema);
        } else if (object instanceof Garage) {
            return createGarageRecord((Garage) object, schema);
        }
        return null;
    }

    @Nullable
    private Schema getSchema(Object object) {
        if (object instanceof Computer) {
            return ReflectData.AllowNull.get().getSchema(Computer.class);
        } else if (object instanceof Card) {
            return ReflectData.AllowNull.get().getSchema(Card.class);
        } else if (object instanceof Garage) {
            return ReflectData.AllowNull.get().getSchema(Garage.class);
        }
        return null;
    }

    private IndexedRecord createCardRecord(Card card, Schema schema) throws IOException {
        final Gson gson = new GsonBuilder().serializeNulls().create();
        final String json = gson.toJson(card);
        return new JsonAvroConverter().convertToGenericDataRecord(json.getBytes(StandardCharsets.UTF_8), schema);
    }

    private IndexedRecord createPrimitveRecord(Primitives primitives, Schema schema) throws IOException {
        final Gson gson = new GsonBuilder().serializeNulls().create();
        final String json = gson.toJson(primitives);
        return new JsonAvroConverter().convertToGenericDataRecord(json.getBytes(StandardCharsets.UTF_8), schema);
    }

    private IndexedRecord createComputerRecord(Computer computer, Schema schema) {
        GenericRecord avroRecord = new GenericData.Record(schema);

        avroRecord.put("name", computer.name);
        avroRecord.put("memory", computer.memory);
        return avroRecord;
    }

    private IndexedRecord createGarageRecord(final Garage garage, Schema schema) throws IOException {
        final Gson gson = new GsonBuilder().serializeNulls().create();
        final String json = gson.toJson(garage);
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
        Garage(String name, List<Car> cars, Map<String, Part> partMap, GarageType type, boolean isOpen) {
            this.name = name;
            this.cars = cars;
            this.partMap = partMap;
            this.type = type;
            this.isOpen = isOpen;
        }

        enum GarageType {
            CAR, TRUCK
        }

        final String name;
        List<Car> cars;
        Map<String, Part> partMap;
        final GarageType type;
        final boolean isOpen;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Garage)) return false;
            Garage garage = (Garage) o;
            return isOpen == garage.isOpen && Objects.equals(name, garage.name) && Objects.equals(cars, garage.cars) && Objects.equals(partMap, garage.partMap) && type == garage.type;
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, cars, partMap, type, isOpen);
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

    static class Computer {
        final String name;
        final ByteBuffer memory;


        Computer(String name, ByteBuffer memory) {
            this.name = name;
            this.memory = memory;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Computer)) return false;
            Computer computer = (Computer) o;
            return Objects.equals(name, computer.name) && Objects.equals(memory, computer.memory);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, memory);
        }
    }

    static class Primitives {
        final long longValue;
        final int intValue;
        final short shortValue;
        final byte byteValue;
        final float floatValue;
        final double doubleValue;
        final boolean booleanValue;

        Primitives(long longValue, int intValue, short shortValue, byte byteValue, float floatValue, double doubleValue, boolean booleanValue) {
            this.longValue = longValue;
            this.intValue = intValue;
            this.shortValue = shortValue;
            this.byteValue = byteValue;
            this.floatValue = floatValue;
            this.doubleValue = doubleValue;
            this.booleanValue = booleanValue;
        }
    }
}
