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
        return new Gson().toJson(structMap(struct));
    }

    private static Map<String, Object> structMap(final Struct struct) {
        final Map<String, Object> dataMap = new LinkedHashMap<>(struct.schema().fields().size());

        for (Field field : struct.schema().fields()) {
            if (field.schema().type() == Schema.Type.STRUCT) {
                final Struct fieldStruct = struct.getStruct(field.name());
                dataMap.put(field.name(), structMap(fieldStruct));
            }else if (field.schema().type() == Schema.Type.ARRAY){
                final List<Object> fieldArray = struct.getArray(field.name());
                final List<Object> fieldJsonArray = jsonArrayFrom(fieldArray);
                dataMap.put(field.name(), fieldJsonArray);
            }else if(field.schema().type() == Schema.Type.MAP){
                final Map<String,Object> jsonMap =  mapFrom(struct.getMap(field.name()));
                dataMap.put(field.name(), jsonMap);
            }else {
                dataMap.put(field.name(), struct.get(field));
            }
        }
        return dataMap;
    }

    private static Map<String, Object> mapFrom(Map<Object, Object> map) {
        final Map<String, Object> jsonMap = new LinkedHashMap<>(map.size());
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
            if(entry.getValue() instanceof Struct){
                jsonMap.put(entry.getKey().toString(), structMap((Struct) entry.getValue()));
            }else if (entry.getValue() instanceof List){
                jsonMap.put(entry.getKey().toString(), jsonArrayFrom((List<Object>) entry.getValue()));
            }else if (entry.getValue() instanceof Map){
                jsonMap.put(entry.getKey().toString(), mapFrom((Map<Object, Object>) entry.getValue()));
            }
        }
        return jsonMap;
    }

    private static List<Object> jsonArrayFrom(List<Object> fieldArray) {
        final List<Object> fieldJsonArray = new ArrayList<>(fieldArray.size());
        for (Object fieldArrayItem : fieldArray) {
            if(fieldArrayItem instanceof Struct){
                fieldJsonArray.add(structMap((Struct) fieldArrayItem));
            }else if (fieldArrayItem instanceof Map){
                fieldJsonArray.add(mapFrom((Map<Object, Object>) fieldArrayItem));
            }else {
                fieldJsonArray.add(fieldArrayItem);
            }
        }
        return fieldJsonArray;
    }
}
