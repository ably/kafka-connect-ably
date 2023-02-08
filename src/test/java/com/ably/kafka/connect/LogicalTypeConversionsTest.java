package com.ably.kafka.connect;

import com.ably.kafka.connect.utils.StructToJsonConverter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
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
    private final static String schemaSubject = "topic-value";
    private final static String topic ="topic";
    private static final Gson gson = new GsonBuilder().serializeNulls().create();

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
    public void testUUIDLogicalType() throws IOException, RestClientException {
        //given
        final String uuidSchemaPath = "/avro_uuid_schema.avsc";
        final String uuidDataPath = "/avro_data_with_uuid.json";
        final Struct struct = structFromAvro(topic, uuidSchemaPath, uuidDataPath);

        //we are expecting the same json
        final JsonElement expected = JsonParser.parseString(readText(uuidDataPath));

        //when
        final String jsonOutput = StructToJsonConverter.toJsonString(struct, gson);
        final JsonElement output = JsonParser.parseString(jsonOutput);
        //then
        assertEquals(expected, output);
    }
    @Test
    public void testSimpleDecimalLogicalType() throws IOException, RestClientException {
        //given
        final String decimalSchemaPath = "/avro_decimal_schema.avsc";
        final String decimalDataPath = "/avro_data_with_decimal.json";
        final Struct struct = structFromAvro(topic, decimalSchemaPath, decimalDataPath);

        //we are expecting the same json
        final JsonElement expected = JsonParser.parseString(readText(decimalDataPath));

        //when
        final String jsonOutput = StructToJsonConverter.toJsonString(struct, gson);
        final JsonElement output = JsonParser.parseString(jsonOutput);
        //then
        assertEquals(expected, output);
    }
    @Test
    public void testDate() throws IOException, RestClientException {
        //given
        final String dateSchemaPath = "/avro_date_schema.avsc";
        final String dateDataPath = "/avro_data_with_date.json";
        final Struct struct = structFromAvro(topic, dateSchemaPath, dateDataPath);

        final JsonElement expected = JsonParser.parseString(readText(dateDataPath));

        //when
        final String jsonOutput = StructToJsonConverter.toJsonString(struct, gson);
        final JsonElement output = JsonParser.parseString(jsonOutput);
        //then
        assertEquals(expected, output);
    }

    @Test
    public void testTimeMillis() throws IOException, RestClientException {
        //given
        final String timeMillisSchema = "/avro_time_millis_schema.avsc";
        final String timeMillisDataPath = "/avro_data_with_time_millis.json";
        final Struct struct = structFromAvro(topic, timeMillisSchema, timeMillisDataPath);

        final JsonElement expected = JsonParser.parseString(readText(timeMillisDataPath));

        //when
        final String jsonOutput = StructToJsonConverter.toJsonString(struct, gson);
        final JsonElement output = JsonParser.parseString(jsonOutput);
        //then
        assertEquals(expected, output);
    }

    @Test
    public void testTimeMicros() throws IOException, RestClientException {
        //given
        final String timeMicrosSchema = "/avro_time_micros_schema.avsc";
        final String timeMicrosDataPath = "/avro_data_with_time_micros.json";
        final Struct struct = structFromAvro(topic, timeMicrosSchema, timeMicrosDataPath);

        final JsonElement expected = JsonParser.parseString(readText(timeMicrosDataPath));

        //when
        final String jsonOutput = StructToJsonConverter.toJsonString(struct, gson);
        final JsonElement output = JsonParser.parseString(jsonOutput);
        //then
        assertEquals(expected, output);
    }

    private String readText(String path) throws IOException {
        final URL url = LogicalTypeConversionsTest.class.getResource(path);
        return IOUtils.toString(url, StandardCharsets.UTF_8);
    }
    private Struct structFromAvro(String topic, String schemaPath, String contentPath) throws IOException, RestClientException {
        final String schemaContent = readText(schemaPath);
        final String valueContent = readText(contentPath);

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
