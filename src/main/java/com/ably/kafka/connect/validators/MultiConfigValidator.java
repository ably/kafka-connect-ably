package com.ably.kafka.connect.validators;

import org.apache.kafka.common.config.ConfigDef;

import javax.annotation.Nonnull;

/*
Currently connector configurations take only a single validator per configuration. This class intends to combine and
support multiple validators.
* */
public class MultiConfigValidator implements ConfigDef.Validator {
    private final ConfigDef.Validator[] validators;

    /**
     * @param validators validators to use. Please provide them by their priority of importance.
     */
    public MultiConfigValidator(@Nonnull ConfigDef.Validator[] validators) {
        this.validators = validators;
    }

    @Override
    public void ensureValid(String name, Object value) {
        for (ConfigDef.Validator validator : validators) {
            validator.ensureValid(name, value);
        }
    }
}
