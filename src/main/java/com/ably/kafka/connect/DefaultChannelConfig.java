package com.ably.kafka.connect;

import io.ably.lib.types.AblyException;
import io.ably.lib.types.ChannelMode;
import io.ably.lib.types.ChannelOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ably.kafka.connect.ChannelSinkConnectorConfig.CHANNEL_CONFIG;
import static com.ably.kafka.connect.ChannelSinkConnectorConfig.CLIENT_CHANNEL_CIPHER_KEY;
import static com.ably.kafka.connect.ChannelSinkConnectorConfig.CLIENT_CHANNEL_PARAMS;

class DefaultChannelConfig implements ChannelConfig {
    private final ChannelSinkConnectorConfig sinkConnectorConfig;

    public DefaultChannelConfig(final ChannelSinkConnectorConfig sinkConnectorConfig) {
        this.sinkConnectorConfig = sinkConnectorConfig;
    }

    @Override
    public String getName() {
        return sinkConnectorConfig.getString(CHANNEL_CONFIG);
    }

    public ChannelOptions getOptions() throws ChannelSinkConnectorConfig.ConfigException {
        final Logger logger = LoggerFactory.getLogger(ChannelSinkConnectorConfig.class);
        ChannelOptions channelOptions;
        final String cipherKey = sinkConnectorConfig.getString(CLIENT_CHANNEL_CIPHER_KEY);

        if (cipherKey != null) {
            try {
                channelOptions = ChannelOptions.withCipherKey(cipherKey);
            } catch (AblyException e) {
                logger.error("Error configuring channel cipher key", e);
                throw new ChannelSinkConnectorConfig.ConfigException("Error configuring channel cipher key", e);
            }
        } else {
            channelOptions = new ChannelOptions();
        }

        // Since we're only publishing, set the channel mode to publish only
        channelOptions.modes = new ChannelMode[]{ChannelMode.publish};
        channelOptions.params = getParams(sinkConnectorConfig.getList(CLIENT_CHANNEL_PARAMS));
        return channelOptions;
    }

    private Map<String, String> getParams(final List<String> params) throws ChannelSinkConnectorConfig.ConfigException {
        final Logger logger = LoggerFactory.getLogger(ChannelSinkConnectorConfig.class);

        final Map<String, String> parsedParams = new HashMap<>();
        for (final String param : params) {
            final String[] parts = param.split("=");
            if (parts.length == 2) {
                final String paramKey = parts[0];
                final String paramVal = parts[1];
                parsedParams.put(paramKey, paramVal);
            } else {
                ChannelSinkConnectorConfig.ConfigException e = new ChannelSinkConnectorConfig.ConfigException(String.format("invalid param string %s", param));
                logger.error("invalid param in channel params configuration", e);
                throw e;
            }
        }

        return parsedParams;
    }
}
