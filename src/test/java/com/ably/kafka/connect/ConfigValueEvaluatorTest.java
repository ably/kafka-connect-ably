package com.ably.kafka.connect;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ConfigValueEvaluatorTest {
    private ConfigValueEvaluator configValueEvaluator = new ConfigValueEvaluator();

    @Test
    void testEvaluateNoKeyThrowsIllegalArgumentException() {
        //given
        SinkRecord sinkRecord = new SinkRecord("topic", 0, null, null, null, null, 0);

       //then throws IllegalArgumentException when
        Exception exception = assertThrows(IllegalArgumentException.class, () -> configValueEvaluator.evaluate(sinkRecord, "anypattern"));
        assertEquals("Key cannot be null", exception.getMessage());
    }

    @Test
    void testEvaluateNoTopicThrowsIllegalArgumentException() {
        //given
        SinkRecord sinkRecord = new SinkRecord(null, 0, null, "key".getBytes(), null, null, 0);

        //then throws IllegalArgumentException when
        Exception exception = assertThrows(IllegalArgumentException.class, () -> configValueEvaluator.evaluate(sinkRecord, "anypattern"));
        assertEquals("Topic cannot be null", exception.getMessage());
    }

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
        SinkRecord sinkRecord = new SinkRecord("greatTopic", 0, null, "key".getBytes(), null, null, 0);

        //when
        String result = configValueEvaluator.evaluate(sinkRecord, "${topic}_hello");

        //then
        assertEquals("greatTopic_hello", result);
    }

    @Test
    void testEvaluateKeyIsEvaluated() {
        //given
        SinkRecord sinkRecord = new SinkRecord("greatTopic", 0, null, "greatKey".getBytes(), null, null, 0);

        //when
        String result = configValueEvaluator.evaluate(sinkRecord, "${key}_hello");

        //then
        assertEquals("greatKey_hello", result);
    }

    @Test
    void testEvaluateKeyAndTopicIsEvaluated() {
        //given
        SinkRecord sinkRecord = new SinkRecord("greatTopic", 0, null, "greatKey".getBytes(), null, null, 0);

        //when
        String result = configValueEvaluator.evaluate(sinkRecord, "key=${key},topic=${topic}");

        //then
        assertEquals("key=greatKey,topic=greatTopic", result);
    }

}