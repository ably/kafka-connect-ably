package com.ably.kafka.connect.utils;

import com.google.gson.Gson;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class StructUtils {
    public static String toJsonString(final Struct struct) {
        return new Gson().toJson(dataMap(struct));
    }

    private static Map<String, Object> dataMap(final Struct struct) {
        final Map<String, Object> dataMap = new LinkedHashMap<>(struct.schema().fields().size());

        for (Field field : struct.schema().fields()) {
            if (field.schema().type() == Schema.Type.STRUCT) {
                final Struct fieldStruct = struct.getStruct(field.name());
                dataMap.put(field.name(), dataMap(fieldStruct));
            }else if (field.schema().type() == Schema.Type.ARRAY){
                final List<Object> fieldArray = struct.getArray(field.name());
                final List<Object> fieldJsonArray = new ArrayList<>(fieldArray.size());
                for (Object fieldArrayItem : fieldArray) {
                    if(fieldArrayItem instanceof Struct){
                        fieldJsonArray.add(dataMap((Struct) fieldArrayItem));
                    }else {
                        fieldJsonArray.add(fieldArrayItem);
                    }
                }
                dataMap.put(field.name(), fieldJsonArray);
            }else {
                dataMap.put(field.name(), struct.get(field));
            }
        }
        return dataMap;
    }
}
