package com.ably.kafka.connect.utils;

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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
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

    private IndexedRecord createCardRecord(Card card, Schema schema) throws IOException {
        GenericRecord avroRecord = new GenericData.Record(schema);

        avroRecord.put("cardId", card.cardId);
        avroRecord.put("limit", card.limit);
        avroRecord.put("pocketId", card.pocketId);
        avroRecord.put("cvv", card.cvv);
        return avroRecord;
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
}
