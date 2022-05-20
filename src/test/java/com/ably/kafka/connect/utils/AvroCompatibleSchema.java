package com.ably.kafka.connect.utils;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroCompatibleSchema implements Schema {
    final org.apache.avro.Schema avroSchema;
    final Map<String, Field> fieldsByName = new HashMap<>();

    public AvroCompatibleSchema(org.apache.avro.Schema avroSchema) {
        this.avroSchema = avroSchema;
    }

    @Override
    public Type type() {
        return Type.STRUCT;
    }

    @Override
    public boolean isOptional() {
        return false;
    }

    @Override
    public Object defaultValue() {
        return null;
    }

    @Override
    public String name() {
        return avroSchema.getName();
    }

    @Override
    public Integer version() {
        return 1;
    }

    @Override
    public String doc() {
        return null;
    }

    @Override
    public Map<String, String> parameters() {
        return null;
    }

    @Override
    public Schema keySchema() {
        return null;
    }

    @Override
    public Schema valueSchema() {
        return this; //this is the same as valueSchema()
    }

    @Override
    public List<Field> fields() {
        final List<org.apache.avro.Schema.Field> avroSchemaFields = avroSchema.getFields();
        final List<Field> fields = new ArrayList<>(avroSchemaFields.size());
        for (int i = 0; i < avroSchemaFields.size(); i++) {
            final org.apache.avro.Schema.Field avroField = avroSchemaFields.get(i);
            final Field field = new Field(avroField.name(), i, toConnectSchema(avroField.schema()));
            fields.add(field);
            fieldsByName.put(avroField.name(), field);
        }

        return fields;
    }

    /**
     * This is just for mock behavior please do not trust this function
     */
    private Schema toConnectSchema(org.apache.avro.Schema schema) {
        switch (schema.getType()) {
            case RECORD:
                return new ConnectSchema(Type.STRUCT);
            case ENUM:
                return new ConnectSchema(Type.STRING);
            case ARRAY:
                return new ConnectSchema(Type.ARRAY);
            case MAP:
                return new ConnectSchema(Type.MAP);
            case UNION:
                return new ConnectSchema(Type.STRUCT);
            case FIXED:
                return new ConnectSchema(Type.BYTES);
            case STRING:
                return new ConnectSchema(Type.STRING);
            case BYTES:
                return new ConnectSchema(Type.BYTES);
            case INT:
                return new ConnectSchema(Type.INT32);
            case LONG:
                return new ConnectSchema(Type.INT64);
            case FLOAT:
                return new ConnectSchema(Type.FLOAT32);
            case DOUBLE:
                return new ConnectSchema(Type.FLOAT64);
            case BOOLEAN:
                return new ConnectSchema(Type.BOOLEAN);
            case NULL:
                new ConnectSchema(Type.STRUCT);
        }
        return ConnectSchema.STRING_SCHEMA;
    }

    @Override
    public Field field(String s) {

        return fieldsByName.get(s);
    }

    @Override
    public Schema schema() {
        return this;
    }
}
