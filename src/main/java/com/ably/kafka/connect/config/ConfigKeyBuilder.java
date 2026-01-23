package com.ably.kafka.connect.config;


import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConfigKeyBuilder {
    private final String name;
    private final ConfigDef.Type type;
    private String documentation = "";
    private Object defaultValue = ConfigDef.NO_DEFAULT_VALUE;
    private ConfigDef.Validator validator;
    private ConfigDef.Importance importance;
    private ConfigDef.Recommender recommender;

    private ConfigKeyBuilder(String name, ConfigDef.Type type) {
        this.name = name;
        this.type = type;
    }

    public static ConfigKeyBuilder of(String name, ConfigDef.Type type) {
        return new ConfigKeyBuilder(name, type);
    }

    public ConfigDef.ConfigKey build() {
        Preconditions.checkState(!Strings.isNullOrEmpty(this.name), "name must be specified.");
        return new ConfigDef.ConfigKey(
            this.name,
            this.type,
            this.defaultValue,
            this.validator,
            this.importance,
            this.documentation,
            "", // group
            -1, // orderInGroup
            ConfigDef.Width.NONE, // width
            this.name, // displayName
            Collections.emptyList(), // dependents
            this.recommender,
            true // internalConfig
        );
    }

    public String name() {
        return this.name;
    }

    public ConfigDef.Type type() {
        return this.type;
    }

    public ConfigKeyBuilder documentation(String documentation) {
        this.documentation = documentation;
        return this;
    }

    public ConfigKeyBuilder defaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    public ConfigKeyBuilder validator(ConfigDef.Validator validator) {
        this.validator = validator;
        return this;
    }

    public ConfigKeyBuilder enumValidator(Class<?> enumClass) {
        return validator(new EnumValidator(enumClass));
    }

    public ConfigKeyBuilder importance(ConfigDef.Importance importance) {
        this.importance = importance;
        return this;
    }

    public ConfigKeyBuilder recommender(ConfigDef.Recommender recommender) {
        this.recommender = recommender;
        return this;
    }

    public ConfigKeyBuilder enumRecommender(Class<?> enumClass) {
        return recommender(new EnumRecommender(enumClass));
    }

    private static class EnumRecommender implements ConfigDef.Recommender {

        private final Class<?> enumClass;

        private EnumRecommender(Class<?> enumClass) {
            this.enumClass = enumClass;
        }

        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            List<Object> validValues = new ArrayList<>();
            for (Object enumValue : enumClass.getEnumConstants()) {
                validValues.add(enumValue.toString());
            }
            return validValues;
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return true;
        }
    }

    private static class EnumValidator implements ConfigDef.Validator {

        private final Class<?> enumClass;
        private final Set<String> validEnums;

        private EnumValidator(Class<?> enumClass) {
            this.enumClass = enumClass;
            this.validEnums = new HashSet<>();
            for (Object enumValue : enumClass.getEnumConstants()) {
                validEnums.add(enumValue.toString());
            }
        }

        @Override
        public void ensureValid(String name, Object value) {
            if (value instanceof String) {
                if (!validEnums.contains(value)) {
                    throw new ConfigException(
                        name,
                        String.format(
                            "'%s' is not a valid value for %s. Valid values are %s.",
                            value,
                            enumClass.getSimpleName(),
                            validEnums
                        )
                    );
                }
            } else if (value instanceof List) {
                for (Object listEntry : (List<?>) value) {
                    ensureValid(name, listEntry);
                }
            } else {
                throw new ConfigException(
                    name,
                    value,
                    "Must be a String or List"
                );
            }

        }
    }
}
