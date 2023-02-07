package com.ably.kafka.connect;

import com.ably.kafka.connect.utils.StructToJsonConverter;
import com.google.gson.Gson;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LogicalTypeConversionsTest {
    final String schemaSubject = "topic-value";

    private SchemaRegistryClient schemaRegistry;
    private AvroConverter converter;
    private KafkaAvroSerializer serializer;


    @BeforeEach
    public void setUp(){
        schemaRegistry = new MockSchemaRegistryClient();

        serializer = new KafkaAvroSerializer(schemaRegistry);
        converter = new AvroConverter(schemaRegistry);
        converter.configure(Map.of("channel","hello","schema.registry.url","bogus"),false);
    }

    @AfterEach
    public void tearDown() throws RestClientException, IOException {
        schemaRegistry.deleteSubject(schemaSubject);
    }

    @Test
    public void readFiles() throws IOException, RestClientException {
        final String topic = "topic";

        final Struct struct = structFromAvro(topic,"/avro_uuid_schema.avsc","/avro_data_with_uuid.json");
        final String json = StructToJsonConverter.toJsonString(struct, new Gson());
        assertEquals(null, json);

    }

    private Struct structFromAvro(String topic, String schemaPath, String contentPath) throws IOException, RestClientException {
        final URL schemaUrl = LogicalTypeConversionsTest.class.getResource(schemaPath);
        final URL valueUrl = LogicalTypeConversionsTest.class.getResource(contentPath);

        final String schemaContent = IOUtils.toString(schemaUrl, StandardCharsets.UTF_8);
        final String valueContent = IOUtils.toString(valueUrl, StandardCharsets.UTF_8);

        final Schema.Parser parser = new Schema.Parser();
        final Schema schema = parser.parse(schemaContent);

        schemaRegistry.register(schemaSubject, new AvroSchema(schema));

        final JsonAvroConverter avroConverter =new  JsonAvroConverter();
        final GenericData.Record record = avroConverter.convertToGenericDataRecord(valueContent.getBytes(StandardCharsets.UTF_8),
            schema);
        final byte[] serialized = serializer.serialize(topic, record);

        final SchemaAndValue schemaAndValue = converter.toConnectData(topic, serialized);
        return  (Struct) schemaAndValue.value();
    }

}
