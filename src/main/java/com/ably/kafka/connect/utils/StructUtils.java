package com.ably.kafka.connect.utils;

import com.google.gson.Gson;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

import java.util.LinkedHashMap;
import java.util.Map;

public class StructUtils {
    public static String toJsonString(final Struct struct) {
        final Map<String, Object> dataMap = new LinkedHashMap<>(struct.schema().fields().size());
        for (Field field : struct.schema().fields()) {
            dataMap.put(field.name(), struct.get(field));
        }
        return new Gson().toJson(dataMap);
    }
}
