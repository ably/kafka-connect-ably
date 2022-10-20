package com.ably.kafka.connect;

import com.ably.kafka.connect.config.ConfigValueEvaluator;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ConfigValueEvaluatorTest {
    private ConfigValueEvaluator configValueEvaluator = new ConfigValueEvaluator();

    @Test
    void testEvaluateStaticValuesStaysSame() {
        //given
        SinkRecord sinkRecord = new SinkRecord("topic", 0, null, "key".getBytes(), null, null, 0);

       //when
        ConfigValueEvaluator.Result result = configValueEvaluator.evaluate(sinkRecord, "containsnopattern", false);

        //then
        assertEquals("containsnopattern", result.getValue());
    }

    @Test
    void testEvaluateTopicIsEvaluated() {
        //given
        SinkRecord sinkRecord = new SinkRecord("greatTopic", 0, null, null, null, null, 0);

        //when
        ConfigValueEvaluator.Result result = configValueEvaluator.evaluate(sinkRecord, "#{topic}_hello", false);

        //then
        assertEquals("greatTopic_hello", result.getValue());
    }

    @Test
    void testEvaluateKeyIsEvaluated() {
        //given
        SinkRecord sinkRecord = new SinkRecord("greatTopic", 0, null,"greatKey".getBytes(), null, null, 0);

        //when
        ConfigValueEvaluator.Result result = configValueEvaluator.evaluate(sinkRecord, "#{key}_hello", false);

        //then
        assertEquals("greatKey_hello", result.getValue());
    }

    @Test
    void testEvaluateKeyAndTopicIsEvaluated() {
        //given
        SinkRecord sinkRecord = new SinkRecord("greatTopic", 0, null, "greatKey".getBytes(), null, null, 0);

        //when
        ConfigValueEvaluator.Result result = configValueEvaluator.evaluate(sinkRecord, "key=#{key},topic=#{topic}", false);

        //then
        assertEquals("key=greatKey,topic=greatTopic", result.getValue());
    }

    @Test
    void testEvaluateNoKeyWithKeyTokenThrowsIllegalArgumentException() {
        //given
        SinkRecord sinkRecord = new SinkRecord("topic", 0, null, null, null, null, 0);
        final String pattern = "pattern_#{key}";

        //then throws IllegalArgumentException when
        Exception exception = assertThrows(IllegalArgumentException.class, () -> configValueEvaluator.evaluate(sinkRecord, pattern, false));

        //then
        assertEquals("Key is null or not a string type but pattern contains #{key}", exception.getMessage());
    }

    @Test
    void testEvaluateInvalidKeyWithKeyTokenThrowsIllegalArgumentException() {
        //given
        //https://stackoverflow.com/a/58210557
        final byte[] nonUTF8Key = new byte[]{(byte)0xC0,  (byte)0xC1, (byte)0xF5, (byte)0xF6, (byte)0xF7, (byte)0xF8};
        SinkRecord sinkRecord = new SinkRecord("topic", 0, null, nonUTF8Key, null, null, 0);
        final String pattern = "pattern_#{key}";

        //then throws IllegalArgumentException when
        Exception exception = assertThrows(IllegalArgumentException.class, () -> configValueEvaluator.evaluate(sinkRecord, pattern, false));

        //then
        assertEquals("Key is null or not a string type but pattern contains #{key}", exception.getMessage());
    }

    @Test
    void testEvaluateNullPatternReturnsNull() {
        //given
        SinkRecord sinkRecord = new SinkRecord("topic", 0, null, "key".getBytes(), null, null, 0);

        //when
        ConfigValueEvaluator.Result result = configValueEvaluator.evaluate(sinkRecord, null, false);

        //then
        assertNull(result.getValue());
    }

    @Test
    void testEvaluateKeyIsEvaluatedNormallyWhenSkippableRecordIsProvided() {
        //given
        SinkRecord sinkRecord = new SinkRecord("greatTopic", 0, null,"greatKey".getBytes(), null, null, 0);

        //when
        ConfigValueEvaluator.Result result = configValueEvaluator.evaluate(sinkRecord, "#{key}_hello", true);

        //then
        assertEquals("greatKey_hello", result.getValue());
    }

    @Test
    void testEvaluateSkipResultIsReturnedWhenRecordWithoutKeyWasProvided() {
        //given
        SinkRecord sinkRecord = new SinkRecord("greatTopic", 0, null,null, null, null, 0);

        //when
        ConfigValueEvaluator.Result result = configValueEvaluator.evaluate(sinkRecord, "#{key}_hello", true);

        //then
        assertTrue(result.shouldSkip());
        assertNull(result.getValue());
    }

    //make sure that #{topic} won't impact the behaviour
    @Test
    void testEvaluateSkipResultfIsReturnedWhenKeyIsNullAndRecordIsInPattern() {
        //given
        SinkRecord sinkRecord = new SinkRecord("greatTopic", 0, null,null, null, null, 0);

        //when
        ConfigValueEvaluator.Result result = configValueEvaluator.evaluate(sinkRecord, "#{key}_hello_#{topic}", true);

        //then
        assertTrue(result.shouldSkip());
        assertNull(result.getValue());
    }

}
