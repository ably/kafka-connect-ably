package com.ably.kafka.connect.utils;

import org.apache.kafka.connect.data.Schema;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
class LogicalTypeConversions{

    /*
    *This method will try to get Avro logical type of
    * @param value and if it exists it will return a value that that has a sensible serialization format.
    * @param type is the normal Avro 'type where logical types applies ..
    * Check @link https://avro.apache.org/docs/1.10.2/spec.html#Logical+Types
    * for the reference on logical types.
    * */
     static Object tryGetLogicalValue(final Schema.Type type, final Object value) {
        switch (type){
            case INT8:
            case INT16:
            case INT32:
                if (value instanceof Date) { // date logical type
                    final Date date = (Date) value;
                    LocalDate localDate = date.toInstant().atZone(ZoneOffset.UTC).toLocalDate();
                    return (int)localDate.toEpochDay();
                } else if (value instanceof LocalDate) {// date logical type
                     final LocalDate date = (LocalDate) value;
                     return (int)date.toEpochDay();
                 } else if (value instanceof LocalTime) {//time-millis and time-micros
                     final LocalTime time = (LocalTime) value;
                     return (int) TimeUnit.NANOSECONDS.toMillis(time.toNanoOfDay());
                 }
                break;
            case INT64:
                 if (value instanceof Instant) { // timestamp-millis
                    final Instant time = (Instant) value;
                    return time.toEpochMilli();
                } else if (value instanceof Date) { // timestamp-millis and timestamp-micros
                     final Date date = (Date) value;
                     return date.toInstant().toEpochMilli();
                 } else if (value instanceof LocalDateTime) { // local-timestamp-millis and local-timestamp-micros
                     final LocalDateTime localDateTime = (LocalDateTime) value;
                     final Instant instant = localDateTime.toInstant(ZoneOffset.UTC);
                     return instant.toEpochMilli();
                 }
                break;
            case STRING: //logical type: uuid
                if (value instanceof UUID){
                    final UUID uuid = (UUID) value;
                    return uuid.toString();
                }
                break;
            case BYTES:
                if (value instanceof BigDecimal) { //logical type: decimal
                    return value;
                }

        }
        return value;
    }

}
