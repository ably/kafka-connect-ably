package com.ably.kafka.connect.validators;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This validator should be used to check the format of the channel name.
 * Other priority validators must be used first
 */
public class ChannelNameValidator implements ConfigDef.Validator {
    private static final String INVALID_CHARACTERS = ":,\\s\\[";
    private static final String INVALID_START_CHARS_PATTERN = "^([^" + INVALID_CHARACTERS + "].*)$";

    @Override
    public void ensureValid(String name, Object value) {
        final Pattern pattern = Pattern.compile(INVALID_START_CHARS_PATTERN);

        final Matcher matcher = pattern.matcher((String) value);
        if (!matcher.matches()) {
            throw new ConfigException(name, value, "Channel name is invalid");
        }
    }
}
