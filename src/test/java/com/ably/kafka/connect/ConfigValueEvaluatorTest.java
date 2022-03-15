package com.ably.kafka.connect;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

class ConfigValueEvaluatorTest {
    private ConfigValueEvaluator configValueEvaluator = new ConfigValueEvaluator();

    @Test
    void testEvaluateStaticValuesStaysSame() {
        //given
        SinkRecord sinkRecord = new SinkRecord("topic", 0, null, "key".getBytes(), null, null, 0);

       //when
        String result = configValueEvaluator.evaluate(sinkRecord, "containsnopattern");

        //then
        assertEquals("containsnopattern", result);
    }

    @Test
    void testEvaluateTopicIsEvaluated() {
        //given
        SinkRecord sinkRecord = new SinkRecord("greatTopic", 0, null, null, null, null, 0);

        //when
        String result = configValueEvaluator.evaluate(sinkRecord, "${topic}_hello");

        //then
        assertEquals("greatTopic_hello", result);
    }

    @Test
    void testEvaluateKeyIsEvaluated() {
        //given
        SinkRecord sinkRecord = new SinkRecord("greatTopic", 0, null, Base64.getDecoder().decode("greatKey"), null, null, 0);

        //when
        String result = configValueEvaluator.evaluate(sinkRecord, "${key}_hello");

        //then
        assertEquals("greatKey_hello", result);
    }

    @Test
    void testEvaluateKeyAndTopicIsEvaluated() {
        //given
        SinkRecord sinkRecord = new SinkRecord("greatTopic", 0, null, Base64.getDecoder().decode("greatKey"), null, null, 0);

        //when
        String result = configValueEvaluator.evaluate(sinkRecord, "key=${key},topic=${topic}");

        //then
        assertEquals("key=greatKey,topic=greatTopic", result);
    }

    @Test
    void testEvaluateNoKeyWithKeyTokenThrowsIllegalArgumentException() {
        //given
        SinkRecord sinkRecord = new SinkRecord("topic", 0, null, null, null, null, 0);
        final String pattern = "pattern_${key}";
        //then throws IllegalArgumentException when
        Exception exception = assertThrows(IllegalArgumentException.class, () -> configValueEvaluator.evaluate(sinkRecord, pattern));
        assertEquals("Key is null and pattern contains ${key}", exception.getMessage());
    }

    @Test
    void testEvaluateNullPatternReturnsNull() {
        //given
        SinkRecord sinkRecord = new SinkRecord("topic", 0, null, "key".getBytes(), null, null, 0);

        //when
        String result = configValueEvaluator.evaluate(sinkRecord, null);

        //then
        assertNull(result);
    }

}