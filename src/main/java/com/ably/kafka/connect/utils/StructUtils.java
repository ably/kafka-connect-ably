package com.ably.kafka.connect.utils;

import com.google.gson.Gson;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class StructUtils {
    public static String toJsonString(final Struct struct) {
        return new Gson().toJson(structMap(struct));
    }

    private static Map<String, Object> structMap(final Struct struct) {
        final Map<String, Object> dataMap = new LinkedHashMap<>(struct.schema().fields().size());

        for (Field field : struct.schema().fields()) {
            switch (field.schema().type()) {
                case STRUCT:
                    final Struct fieldStruct = struct.getStruct(field.name());
                    dataMap.put(field.name(), structMap(fieldStruct));
                    break;
                case ARRAY:
                    final List<Object> fieldArray = struct.getArray(field.name());
                    final List<Object> fieldJsonArray = jsonArrayFrom(fieldArray);
                    dataMap.put(field.name(), fieldJsonArray);
                    break;
                case MAP:
                    final Map<String, Object> jsonMap = mapFrom(struct.getMap(field.name()));
                    dataMap.put(field.name(), jsonMap);
                default:
                    System.out.println("Field: " + field.name() + " value: " + struct.get(field));
                    dataMap.put(field.name(), eval(struct.get(field)));
            }
        }
        return dataMap;
    }


    private static Map<String, Object> mapFrom(Map<Object, Object> map) {
        if (map == null) {
            return null;
        }
        final Map<String, Object> jsonMap = new LinkedHashMap<>(map.size());
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
            jsonMap.put(entry.getKey().toString(),eval(entry.getValue()));
        }
        return jsonMap;
    }

    private static List<Object> jsonArrayFrom(List<Object> fieldArray) {
        final List<Object> fieldJsonArray = new ArrayList<>(fieldArray.size());
        for (Object fieldArrayItem : fieldArray) {
            fieldJsonArray.add(eval(fieldArrayItem));
        }
        return fieldJsonArray;
    }

    private static Object eval(final Object value) {
        if (value instanceof Struct) {
            return structMap((Struct) value);
        } else if (value instanceof List) {
            return jsonArrayFrom((List<Object>) value);
        } else if (value instanceof Map) {
            return mapFrom((Map<Object, Object>) value);
        }
        return value;
    }
}
