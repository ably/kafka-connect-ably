package com.ably.kafka.connect.mapping;

import com.ably.kafka.connect.utils.ByteArrayUtils;
import com.google.common.base.Functions;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TemplatedRecordMapping implements RecordMapping {

    private static final Logger logger = LoggerFactory.getLogger(TemplatedRecordMapping.class.getName());

    /**
     * Pattern to template placeholders in config strings
     */
    static final Pattern TEMPLATE_PATTERN = Pattern.compile("#\\{([^}]+)}");

    /**
     * Pattern to validate that a placeholder is a nested field reference
     */
    private static final Pattern NESTED_REFERENCE_PATTERN = Pattern.compile("(key|value)\\..+");

    private static final Map<String, Function<SinkRecord, String>> DIRECT_SUBSTITUTIONS =
        Map.of(
            "topic", SinkRecord::topic,
            "topic.name", SinkRecord::topic,
            "topic.partition", Functions.compose(TemplatedRecordMapping::stringify, SinkRecord::kafkaPartition),
            "key", Functions.compose(TemplatedRecordMapping::stringify, SinkRecord::key)
        );

    /**
     * Scalar schema types that we know how to convert to strings in a reasonable way
     */
    private static final Set<Schema.Type> SUPPORTED_SCALARS =
        Set.of(
            Schema.Type.STRING,
            Schema.Type.INT8, Schema.Type.INT16, Schema.Type.INT32, Schema.Type.INT64,
            Schema.Type.BOOLEAN,
            Schema.Type.BYTES
        );

    private final String template;

    /**
     * If value is one of the types we can sensibly interpret as a String, return a String
     * representation of that value. Else, returns null.
     *
     * @param value scalar to be reinterpreted as a string.
     *
     * @return String representation of value
     */
    private static String stringify(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            return (String) value;
        } else if (value instanceof Integer || value instanceof Long || value instanceof Boolean) {
            return String.valueOf(value);
        } else if (value instanceof byte[] && ByteArrayUtils.isUTF8Encoded((byte[]) value)) {
            return new String((byte[]) value, StandardCharsets.UTF_8);
        } else {
            logger.warn("Don't know how to stringify type {} in record {}", value.getClass(), value);
            return null;
        }
    }

    /**
     * Construct a new templated record mapping for given template string.
     *
     * @param template The placeholder template pattern to use for formatting.
     */
    public TemplatedRecordMapping(String template) {
        // Fail early rather than waiting for runtime data, if:
        //  * this template has no placeholders
        //  * the placeholders are invalid
        if (!hasPlaceholders(template)) {
            throw new IllegalArgumentException("Template contains no placeholders: " + template);
        }
        final Matcher matcher = TEMPLATE_PATTERN.matcher(template);
        while (matcher.find()) {
            final String placeholder = matcher.group(1);
            if (!(DIRECT_SUBSTITUTIONS.containsKey(placeholder) || isNestedFieldReference(placeholder))) {
                throw new IllegalArgumentException("Invalid template: " + template);
            }
        }

        this.template = template;
    }

    /**
     * Check to see if a config string contains any placeholders for substitution
     *
     * @param configPattern Message name or channel name config string
     *
     * @return true if this config string should be templated
     */
    public static boolean hasPlaceholders(final String configPattern) {
        return TEMPLATE_PATTERN.asPredicate().test(configPattern);
    }

    /**
     * Validate that a field reference appears to be a nested reference
     *
     * @param fieldReference The placeholder text, inside #{ ... }
     *
     * @return true if this looks like a valid nested field reference, else false
     */
    private static boolean isNestedFieldReference(final String fieldReference) {
        // note: asMatchPredicate() requires whole string match, i.e. from the start of the string
        return NESTED_REFERENCE_PATTERN.asMatchPredicate().test(fieldReference);
    }

    @Override
    public String map(SinkRecord record) throws RecordMappingException {
        return TEMPLATE_PATTERN
            .matcher(template)
            .replaceAll(matchResult -> substitutionFor(record, matchResult.group(1)));
    }

    /**
     * Looks up a single substitution for a field reference within a record.
     *
     * @param record The sink record containing the data to be formatted in
     * @param fieldReference a single reference placeholder from the template
     * @return the text to be substituted into the template
     */
    private String substitutionFor(final SinkRecord record, final String fieldReference) {
        if (DIRECT_SUBSTITUTIONS.containsKey(fieldReference)) {
            final String mapping = DIRECT_SUBSTITUTIONS.get(fieldReference).apply(record);
            if (mapping == null) {
                mappingError("Record does not have a value for: " + fieldReference);
            }
            return mapping;
        } else if (isNestedFieldReference(fieldReference)) {
            return extractNestedReference(record, fieldReference);
        } else {
            // should be unreachable, patterns are validated in constructor
            throw new IllegalStateException("Don't know how to process field reference: " + fieldReference);
        }
    }

    /**
     * Process a nested field reference, returning the string representation of the referenced value.
     *
     * @param record SinkRecord to extract value from
     * @param fieldReference the dot-separated path to the desired field
     * @return string representation of tail element in field reference
     */
    private String extractNestedReference(final SinkRecord record, final String fieldReference) {
        final String[] referencePath = fieldReference.split("\\.");
        Object rootObject;
        Schema rootSchema;
        if (referencePath[0].equals("key")) {
            rootObject = record.key();
            rootSchema = record.keySchema();
        } else if (referencePath[0].equals("value")) {
            rootObject = record.value();
            rootSchema = record.valueSchema();
        } else {
            // unreachable
            throw new IllegalStateException("Invalid field reference - unknown root");
        }

        final List<String> remaining = Arrays.stream(referencePath).skip(1).collect(Collectors.toList());
        return followReferencePath(rootObject, rootSchema, remaining);
    }

    /**
     * Recurse through a field reference path and return a string representation of the last element.
     *
     * @param rootObject the root value, corresponding to the head of the path
     * @param rootSchema the schema for the root object
     * @param remaining list of remaining fields to extract
     * @return the string representation of the tail element in the path
     */
    private String followReferencePath(
        final Object rootObject,
        final Schema rootSchema,
        final List<String> remaining) {
        if (remaining.isEmpty()) {
            // If we're at the end of the reference path, we should have a scalar value now
            if (SUPPORTED_SCALARS.contains(rootSchema.type())) {
                return stringify(rootObject);
            } else {
                throw new RecordMappingException(
                    "Final element in a field reference must be a string-able scalar, got: " + rootSchema.type()
                );
            }
        } else if (rootSchema == null) {
            // Can't make nested references without a record schema
            throw new RecordMappingException(
                "Template contains a nested field reference but record has no schema"
            );
        } else if (rootSchema.type().equals(Schema.Type.STRUCT)) {
            // If we're not at the end, this should be a struct, so recurse down
            final String fieldName = remaining.get(0);
            final Set<String> fieldNames = rootSchema.fields().stream()
                .map(Field::name)
                .collect(Collectors.toSet());
            if (fieldNames.contains(fieldName)) {
                final Struct struct = (Struct) rootObject;
                return followReferencePath(
                    struct.get(fieldName),
                    rootSchema.field(fieldName).schema(),
                    remaining.subList(1, remaining.size())
                );
            } else {
                throw new RecordMappingException(
                    "Template contains reference to missing field " + fieldName
                );
            }
        } else {
            // Attempted `.` access on a non-struct field
            throw new RecordMappingException(
                "Inner elements in a nested field reference must be structs, found: " + rootSchema.type()
            );
        }
    }

    /**
     * Throws a RecordMappingException with given error.
     *
     * @param message Human-readable message to be sent to DLQ, if configured.
     *
     * @throws RecordMappingException always
     */
    private void mappingError(String message) {
        throw new RecordMappingException(message);
    }
}
