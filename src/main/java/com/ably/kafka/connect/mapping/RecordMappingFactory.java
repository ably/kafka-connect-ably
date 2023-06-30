package com.ably.kafka.connect.mapping;

import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;

import static com.ably.kafka.connect.config.ChannelSinkConnectorConfig.CHANNEL_CONFIG;
import static com.ably.kafka.connect.config.ChannelSinkConnectorConfig.MESSAGE_CONFIG;

public class RecordMappingFactory {

    private final ChannelSinkConnectorConfig connectorConfig;

    /**
     * Set up a record mapping factory for this connector configuration
     *
     * @param connectorConfig The global connector settings
     */
    public RecordMappingFactory(ChannelSinkConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    /**
     * Record to string mapping for message names
     *
     * @return RecordMapping implementation for message.name
     */
    public RecordMapping messageNameMapping() {
        return mappingForPattern(connectorConfig.getString(MESSAGE_CONFIG));
    }

    /**
     * Record to string mapping for channel names
     *
     * @return RecordMapping implementation for channel names
     */
    public RecordMapping channelNameMapping() {
        final String channelMappingPattern = connectorConfig.getString(CHANNEL_CONFIG);
        if (channelMappingPattern == null || channelMappingPattern.isEmpty()) {
            throw new IllegalStateException(String.format("%s configuration must be set", CHANNEL_CONFIG));
        } else {
            return mappingForPattern(channelMappingPattern);
        }
    }

    /**
     * Construct the appropriate type of record mapping for a given config string
     *
     * @param patternConfig The pattern or static string provided in config
     * @return A RecordMapping implementation to handle this pattern type
     */
    private RecordMapping mappingForPattern(final String patternConfig) {
        if (patternConfig == null || patternConfig.isEmpty()) {
            return new NullRecordMapping();
        } else if (TemplatedRecordMapping.hasPlaceholders(patternConfig)) {
            return new TemplatedRecordMapping(patternConfig);
        } else {
            return new StaticRecordMapping(patternConfig);
        }
    }
}
