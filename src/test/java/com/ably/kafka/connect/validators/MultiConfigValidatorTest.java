package com.ably.kafka.connect.validators;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MultiConfigValidatorTest {
    final static String CHANNEL_KEY = "channel";
    private final ChannelNameValidator validator = new ChannelNameValidator();

    @Test
    void testValidChannelName() {
        assertDoesNotThrow(() -> validator.ensureValid(CHANNEL_KEY,"valid_channel"));
    }

    @Test
    void testInvalidChannelName_withStartColomn_ThrowsException() {
        assertThrows(ConfigException.class, () -> validator.ensureValid(CHANNEL_KEY,":invalid_channel"));
    }

    @Test
    void testInvalidChannelName_withStartWithOpeningBracket_ThrowsException() {
        assertThrows(ConfigException.class, () -> validator.ensureValid(CHANNEL_KEY,"[invalid_channel"));
    }
}
