package com.ably.kafka.connect.utils;

import org.apache.kafka.connect.data.Schema;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
public class LogicalTypeConversions{
    /*
    *This method will try to get logical type of
    * @param value and if it exists it will return a value that has a sensible default serialization format
    * @param type is the super type where logical types applies ..
    * Check @link https://avro.apache.org/docs/1.10.2/spec.html#Logical+Types
    * for the reference on logical types.
    * */
    public static Object tryGetLogicalValue(final Schema.Type type, Object value) {
        switch (type){
            case INT8:
            case INT16:
            case INT32:
                 if (value instanceof LocalDate) {
                     // date ->  This applies to integers and is represented as LocalDate of Java
                     final LocalDate date = (LocalDate) value;
                     return (int)date.toEpochDay();
                 } else if (value instanceof LocalTime) {
                     //This applies to time-millis and time-micros
                     final LocalTime time = (LocalTime) value;
                     return (int) TimeUnit.NANOSECONDS.toMillis(time.toNanoOfDay());
                 }
            case INT64:
                 if (value instanceof Instant) {
                     //timestamp-millis and timestamp-micros
                    final Instant time = (Instant) value;
                    return time.toEpochMilli();
                } else if (value instanceof LocalDateTime) {
                     //local-timestamp-millis and local-timestamp-micros
                     final LocalDateTime localDateTime = (LocalDateTime) value;
                     final Instant instant = localDateTime.toInstant(ZoneOffset.UTC);
                     return instant.toEpochMilli();
                 }
                 //duration is not implemented in org.apache.avro.data package - so leaving it here for now
                break;
            case STRING:
                //logical type: uuid
                if (value instanceof UUID){
                    final UUID uuid = (UUID) value;
                    return uuid.toString();
                }
                break;
            case BYTES:
                //logical type: decimal
                if (value instanceof BigDecimal) {
                    //decimal - no need for conversion
                    return value;
                }

        }
        //return the value itself if logical types don't apply
        return value;
    }

}
