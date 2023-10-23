package com.ably.kafka.connect.mapping;

import com.ably.kafka.connect.utils.RecordHeaderConversions;
import com.ably.kafka.connect.utils.SpecialAblyHeader;
import com.ably.kafka.connect.utils.StructToJsonConverter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.ably.lib.types.Message;
import io.ably.lib.types.MessageExtras;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Converts Kafka SinkRecord instances to Ably Messages
 */
public class MessageConverter {

    private static final Gson gson = new GsonBuilder().serializeNulls().create();

    /**
     * Convert a SinkRecord to an Ably Message
     *
     * @param messageName mapped message name, or null if not using names
     * @param record      the incoming Kafka Connect SinkRecord
     */
    public static Message toAblyMessage(final String messageName, final SinkRecord record) {
        if (record.valueSchema() == null) {
            return new Message(messageName, record.value());
        }

        final Schema valueSchema = record.valueSchema();

        Message message;
        switch (valueSchema.type()) {
            case STRUCT:
                final String jsonString = StructToJsonConverter.toJsonString((Struct) record.value(), gson);
                message = new Message(messageName, jsonString);
                message.encoding = "json";
                break;
            case BYTES:
            case STRING:
                message = new Message(messageName, record.value());
                break;
            default:
                throw new ConnectException("Unsupported value schema type: " + valueSchema.type());
        }

        SpecialAblyHeader.ENCODING_HEADER.tryFindInHeaders(record.headers())
            .ifPresent(header -> message.encoding = String.valueOf(header.value()));

        final MessageExtras extras = RecordHeaderConversions.toMessageExtras(record);
        if (extras != null) {
            message.extras = extras;
        }

        return message;
    }

}
