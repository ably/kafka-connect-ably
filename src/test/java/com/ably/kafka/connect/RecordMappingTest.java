package com.ably.kafka.connect;

import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.mapping.RecordMapping;
import com.ably.kafka.connect.mapping.RecordMappingException;
import com.ably.kafka.connect.mapping.RecordMappingFactory;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

public class RecordMappingTest {

    /**
     * Ensure that we allow null values for message name, as it is optional.
     */
    @Test
    public void testCanMapMessageNamesToNull() {
        final RecordMappingFactory factory = configureFactory(
            Map.of(
                "channel", "foo"
                // note: message.name is unset
            )
        );

        final RecordMapping mapping = factory.messageNameMapping();
        assertNull(mapping.map(new SinkRecord("topic1", 0, null, "key1", null, "value1", 0)));
    }

    /**
     * Ensure that channel name will always be set in config
     */
    @Test
    public void testWillNotMapChannelNamesToNull() {
        // Shouldn't even be able to construct config without setting a channel
        assertThrows(ConfigException.class, () -> configureFactory(Map.of("message.name", "bar")));
    }

    /**
     * Test that both message.name and channel can be set to string constants without
     * any placeholders for substitution at all.
     */
    @Test
    public void testStaticChannelAndMessageNameMappings() {
        final RecordMappingFactory factory = configureFactory(
            Map.of(
                "message.name", "static_message_name",
                "channel", "static_channel_name"
            )
        );

        final RecordMapping channelMapping = factory.channelNameMapping();
        final RecordMapping messageMapping = factory.messageNameMapping();
        final SinkRecord record = new SinkRecord("topic1", 0, null, "key1", null, "value1", 0);
        assertEquals("static_channel_name", channelMapping.map(record));
        assertEquals("static_message_name", messageMapping.map(record));
    }

    /**
     * Test that the topic and key aliases used in earlier versions still work
     * with the new record mapping logic in a compatible way
     */
    @Test
    public void testSupportsV2PlaceholderSubstitutions() {
        final RecordMappingFactory factory = configureFactory(
            Map.of(
                "message.name", "message_#{key}_#{topic}_#{key}",
                "channel", "channel_#{key}_#{topic}_#{key}"
            )
        );

        final RecordMapping channelMapping = factory.channelNameMapping();
        final RecordMapping messageMapping = factory.messageNameMapping();
        final SinkRecord record = new SinkRecord("topic2", 0, null, "key2", null, "value2", 0);
        assertEquals("channel_key2_topic2_key2", channelMapping.map(record));
        assertEquals("message_key2_topic2_key2", messageMapping.map(record));
    }

    /**
     * Ensure the simple, top-level aliases for unstructured references can be used in
     * both channel and message name templates
     */
    @Test
    public void testDirectSubstitutionsInMessageAndChannelNames() {
        final RecordMappingFactory factory = configureFactory(
            Map.of(
                "message.name", "message_#{topic.name}_#{topic.partition}_#{topic}_#{key}",
                "channel", "channel_#{topic.name}_#{topic.partition}_#{topic}_#{key}"
            )
        );

        final RecordMapping channelMapping = factory.channelNameMapping();
        final RecordMapping messageMapping = factory.messageNameMapping();
        final SinkRecord record = new SinkRecord("topic3", 1, null, "key3", null, "value3", 0);
        assertEquals("channel_topic3_1_topic3_key3", channelMapping.map(record));
        assertEquals("message_topic3_1_topic3_key3", messageMapping.map(record));
    }

    /**
     * Ensure that template references to key trigger an exception with the configured skip
     * behaviour. Kafka record keys are optional, but referring to the key without telling the
     * Ably connector to skip records with missing keys should stop the connector.
     */
    @Test
    public void testThrowsExceptionOnMissingKeyReference() {
        final SinkRecord record = new SinkRecord("topic4", 2, null, null, null, "value3", 0);

        // If skip is set in config, expect the SKIP_RECORD action on missing keys
        final RecordMappingFactory factoryWithSkip = configureFactory(
            Map.of(
                "message.name", "message_#{key}",
                "channel", "channel_#{key}",
                "skipOnKeyAbsence", "true"
            )
        );
        assertRecordMappingException(RecordMappingException.Action.SKIP_RECORD,
            () -> factoryWithSkip.channelNameMapping().map(record));
        assertRecordMappingException(RecordMappingException.Action.SKIP_RECORD,
            () -> factoryWithSkip.messageNameMapping().map(record));

        // If skip is false, or unset, we should default to stopping the sink task,
        // because skips are a dangerous default and may result in data loss
        final RecordMappingFactory factoryWithoutSkip = configureFactory(
            Map.of(
                "message.name", "message_#{key}",
                "channel", "channel_#{key}"
            )
        );
        assertRecordMappingException(RecordMappingException.Action.STOP_TASK,
            () -> factoryWithoutSkip.channelNameMapping().map(record));
        assertRecordMappingException(RecordMappingException.Action.STOP_TASK,
            () -> factoryWithoutSkip.messageNameMapping().map(record));
    }

    /**
     * Ensure that keys with any schema that we can convert to a string are supported
     */
    @Test
    public void testKeyCanBeAnySupportedScalarType() {
        final RecordMappingFactory factory = configureFactory(
            Map.of(
                "message.name", "message_#{key}",
                "channel", "channel_#{key}"
            )
        );

        final RecordMapping channelMapping = factory.channelNameMapping();
        final RecordMapping messageMapping = factory.messageNameMapping();

        // String key
        final SinkRecord stringKeyRecord = new SinkRecord("topic5", 3, Schema.STRING_SCHEMA, "strKey", null, "value4", 0);
        assertEquals("channel_strKey", channelMapping.map(stringKeyRecord));
        assertEquals("message_strKey", messageMapping.map(stringKeyRecord));

        // Integral key
        for (Schema integralSchema : List.of(
            Schema.INT8_SCHEMA,
            Schema.INT16_SCHEMA,
            Schema.INT32_SCHEMA,
            Schema.INT64_SCHEMA)) {
            final SinkRecord intKeyRecord = new SinkRecord("topic5", 3, integralSchema, 123, null, "value4", 0);
            assertEquals("channel_123", channelMapping.map(intKeyRecord));
            assertEquals("message_123", messageMapping.map(intKeyRecord));
        }

        // Boolean key (sounds like a bad plan)
        final SinkRecord boolKeyRecord = new SinkRecord("topic5", 3, Schema.BOOLEAN_SCHEMA, true, null, "value4", 0);
        assertEquals("channel_true", channelMapping.map(boolKeyRecord));
        assertEquals("message_true", messageMapping.map(boolKeyRecord));

        // UTF-8 bytes key
        final byte[] key = new byte[] {0x48, 0x65, 0x6c, 0x6c, 0x6f};
        final SinkRecord bytesKeyRecord = new SinkRecord("topic5", 3, Schema.BYTES_SCHEMA, key, null, "value4", 0);
        assertEquals("channel_Hello", channelMapping.map(bytesKeyRecord));
        assertEquals("message_Hello", messageMapping.map(bytesKeyRecord));
    }

    /**
     * Ensure that valid references to nested data can be used in channel and message names
     */
    @Test
    public void testNestedFieldReferences() {
        // Schema for a record key
        final Schema keySchema = new SchemaBuilder(Schema.Type.STRUCT)
            .field("id", Schema.INT32_SCHEMA)
            .field("instance", Schema.STRING_SCHEMA)
            .build();

        // Schema for a record value
        final Schema valueSubSchema = new SchemaBuilder(Schema.Type.STRUCT)
            .field("nestedId", Schema.INT64_SCHEMA)
            .build();
        final Schema valueSchema = new SchemaBuilder(Schema.Type.STRUCT)
            .field("nestedStruct", valueSubSchema)
            .build();

        // Set up a struct instance as per the key schema
        final Struct testKey = new Struct(keySchema)
            .put("id", 12345)
            .put("instance", "i-6789");

        // Set up a struct using the value schema
        final Struct testValue = new Struct(valueSchema)
            .put("nestedStruct",
                new Struct(valueSubSchema)
                    .put("nestedId", 12345678L));

        // Create a mapping config referencing nested data
        final RecordMappingFactory nestedRefsFactory = configureFactory(
            Map.of(
                "message.name", "message_#{key.id}_#{key.instance}",
                "channel", "channel_#{value.nestedStruct.nestedId}"
            )
        );
        final RecordMapping nestedRefChannelMapping = nestedRefsFactory.channelNameMapping();
        final RecordMapping nestedRefMessageMapping = nestedRefsFactory.messageNameMapping();
        final SinkRecord nestedRefSinkRecord = new SinkRecord("topic6", 4, keySchema, testKey, valueSchema, testValue, 0);

        assertEquals("channel_12345678", nestedRefChannelMapping.map(nestedRefSinkRecord));
        assertEquals("message_12345_i-6789", nestedRefMessageMapping.map(nestedRefSinkRecord));
    }

    /**
     * References to missing fields should trigger configured skip/stop behaviour
     */
    @Test
    public void testReferenceToMissingField() {
        final RecordMappingFactory factory = configureFactory(
            Map.of(
                "channel", "channel_#{value.cheescake}",
                "skipOnKeyAbsence", "false"
            )
        );
        final RecordMapping channelMapping = factory.channelNameMapping();

        final Schema schema = new SchemaBuilder(Schema.Type.STRUCT)
            .field("id", Schema.INT16_SCHEMA)
            .build();
        final Struct testStruct = new Struct(schema)
            .put("id", (short) 12);

        final SinkRecord record = new SinkRecord("topic7", 5, null, null, schema, testStruct, 0);
        assertRecordMappingException(RecordMappingException.Action.STOP_TASK,
            () -> channelMapping.map(record));
    }

    /**
     * References to fields of the wrong type should trigger skip/stop behaviour
     */
    @Test
    public void testReferenceToNonScalarField() {
        final RecordMappingFactory factory = configureFactory(
            Map.of(
                "channel", "channel_#{key.structField}",
                "message.name", "message_#{key}",
                "skipOnKeyAbsence", "true"
            )
        );

        final RecordMapping channelMapping = factory.channelNameMapping();
        final RecordMapping messageMapping = factory.messageNameMapping();

        final Schema nestedSchema = new SchemaBuilder(Schema.Type.STRUCT)
            .field("id", Schema.INT32_SCHEMA)
            .build();
        final Schema schema = new SchemaBuilder(Schema.Type.STRUCT)
            .field("structField", nestedSchema)
            .build();

        final Struct testStruct = new Struct(schema)
            .put("structField",
                new Struct(nestedSchema)
                    .put("id", 42));

        final SinkRecord record = new SinkRecord("topic7", 5, schema, testStruct, null, "value", 0);
        assertRecordMappingException(RecordMappingException.Action.SKIP_RECORD,
            () -> channelMapping.map(record));
        assertRecordMappingException(RecordMappingException.Action.SKIP_RECORD,
            () -> messageMapping.map(record));
    }

    /**
     * If a record has no schema, but the template makes nested records, we should
     * trigger the configured error handling behaviour
     */
    @Test
    public void testNestedFieldAccessOnSchemalessRecord() {
        final RecordMappingFactory factory = configureFactory(
            Map.of(
                "channel", "channel_#{key.id}",
                "skipOnKeyAbsence", "false"
            )
        );
        final RecordMapping channelMapping = factory.channelNameMapping();
        final SinkRecord record = new SinkRecord("topic8", 5, null, "key", null, "value", 0);
        assertRecordMappingException(RecordMappingException.Action.STOP_TASK,
            () -> channelMapping.map(record));
    }

    /**
     * Check that a RecordMappingException is thrown with the expected error handling action set.
     *
     * @param action the expected error handling action
     * @param fn test code to execute
     */
    private <T> void assertRecordMappingException(RecordMappingException.Action action, Supplier<T> fn) {
        T ret;
        try {
            ret = fn.get();
        } catch (Throwable exc) {
            if (!exc.getClass().equals(RecordMappingException.class)) {
                exc.printStackTrace();
            }
            assertEquals(exc.getClass(), RecordMappingException.class);
            if (exc instanceof RecordMappingException) {
                assertEquals(action, ((RecordMappingException) exc).action());
            }
            return;
        }

        fail("Expected RecordMappingException to be thrown, but nothing was thrown. " +
             "function returned: " + ret.toString());
    }

    /**
     * Configure the RecordMappingFactory for given settings map
     *
     * @param settings connector configuration as string-string map
     *
     * @return RecordMappingFactory configured as per settings
     */
    private static RecordMappingFactory configureFactory(final Map<String, String> settings) {
        final Map<String, String> mutableCopy = new HashMap<>(settings);
        mutableCopy.put("client.key", "unused_key");
        mutableCopy.put("client.id", "unused_id");
        return new RecordMappingFactory(new ChannelSinkConnectorConfig(mutableCopy));
    }
}
