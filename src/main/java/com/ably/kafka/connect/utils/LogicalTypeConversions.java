package com.ably.kafka.connect.utils;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
/***
 * decimal -> This applies to bytes. This is BigDecimal
 * date ->  This applies to integers and is represented as LocalDate of Java
 * time-millis -> This applies to integers and is represented as LocalTime of Java.
 * time-micros -> Same as above
 * timestamp-millis -> Applies to long and is represented as LocalDateTime of Java
 * timestamp-micros -> Same as above
 * uuid -> Applies to Strings and is represented as UUID of Java
 * */
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
                     return date.toEpochDay();
                 } else if (value instanceof LocalTime) {
                     //This applies to time-millis and time-micros
                     final LocalTime time = (LocalTime) value;
                     return time.toSecondOfDay();// todo check this
                 }
            case INT64:
                 if (value instanceof LocalDateTime) {
                     //timestamp-millis and timestamp-micros
                    final LocalDateTime time = (LocalDateTime) value;
                    return time.getSecond(); //todo check this
                }else if (value instanceof Duration){
                     //duration ? //todo check if such a thing exists
                    final Duration duration = (Duration) value;
                    duration.getNano();
                }
                break;
            case STRING:
                //uuid
                if (value instanceof UUID){
                    final UUID uuid = (UUID) value;
                    return uuid.toString();
                }
                break;
            case BYTES:
                //decimal
                if (value instanceof BigDecimal) {
                    //decimal - no need for conversion
                    return value;
                }

        }
        //return the value itself if logical types don't apply
        return value;
    }

}
