package com.ably.kafka.connect.utils;

import com.google.gson.Gson;
import org.apache.avro.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

class StructUtilsTest {

    @Test
    void testStructToJsonString() throws IOException {
        //given
        final Card card = new Card("cardId", 10000, "pocketId", "123");
        final byte[] schemaData = StructUtilsTest.class.getResource("/example_schema.json").openStream().readAllBytes();
        final String schemaContent = new String(schemaData, StandardCharsets.UTF_8);

        final Schema.Parser parser = new Schema.Parser();
        final Schema avroSchema = parser.parse(schemaContent);
        final AvroCompatibleSchema valueSchema = new AvroCompatibleSchema(avroSchema);

        final Struct struct = new Struct(valueSchema);
        struct.put("cardId", card.cardId);
        struct.put("limit", card.limit);
        struct.put("pocketId", card.pocketId);
        struct.put("cvv", card.cvv);


        //when
        final String jsonString = StructUtils.toJsonString(struct);

        //then
        final Card receivedCard = new Gson().fromJson(jsonString, Card.class);
        assertEquals(receivedCard, card);
    }


    static class Card {
        private String cardId;
        private int limit;
        private String pocketId;
        private String cvv;

        public Card(String cardId, int limit, String pocketId, String cvv) {
            this.cardId = cardId;
            this.limit = limit;
            this.pocketId = pocketId;
            this.cvv = cvv;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Card)) return false;
            Card card = (Card) o;
            return limit == card.limit && cardId.equals(card.cardId) && pocketId.equals(card.pocketId) && cvv.equals(card.cvv);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cardId, limit, pocketId, cvv);
        }
    }
}
