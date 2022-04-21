package com.ably.kafka.connect.config;

import com.ably.kafka.connect.ChannelSinkTask;
import io.ably.lib.types.AblyException;
import io.ably.lib.types.ChannelMode;
import io.ably.lib.types.ChannelOptions;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ably.kafka.connect.config.ChannelSinkConnectorConfig.CHANNEL_CONFIG;
import static com.ably.kafka.connect.config.ChannelSinkConnectorConfig.CIPHER_KEY_CLASS;
import static com.ably.kafka.connect.config.ChannelSinkConnectorConfig.CLIENT_CHANNEL_CIPHER_KEY;
import static com.ably.kafka.connect.config.ChannelSinkConnectorConfig.CLIENT_CHANNEL_PARAMS;

public class DefaultChannelConfig implements ChannelConfig {
    private static final Logger logger = LoggerFactory.getLogger(DefaultChannelConfig.class);

    private final ChannelSinkConnectorConfig sinkConnectorConfig;

    public DefaultChannelConfig(final ChannelSinkConnectorConfig sinkConnectorConfig) {
        this.sinkConnectorConfig = sinkConnectorConfig;
    }

    @Override
    public String getName() {
        return sinkConnectorConfig.getString(CHANNEL_CONFIG);
    }

    public ChannelOptions getOptions(SinkRecord record) throws ChannelSinkConnectorConfig.ConfigException {
        ChannelOptions channelOptions;
         String cipherKey = sinkConnectorConfig.getString(CLIENT_CHANNEL_CIPHER_KEY);

        final Object cipherConfigInstance = Utils.newInstance(sinkConnectorConfig.getClass(CIPHER_KEY_CLASS));
        if (cipherConfigInstance instanceof CipherConfig) {
            final CipherConfig cipherconfig = (CipherConfig) cipherConfigInstance;
            cipherKey = cipherconfig.key(record);
        }else {
            //ignore for now
           // throw new ChannelSinkConnectorConfig.ConfigException(String.format("invalid cipher key class %s", CIPHER_KEY_CLASS));
        }


        if (cipherKey != null) {
            logger.info("Using cipher key {}", cipherKey);
            try {
                channelOptions = ChannelOptions.withCipherKey(cipherKey);
            } catch (AblyException e) {
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
        final Map<String, String> parsedParams = new HashMap<>();
        for (final String param : params) {
            final String[] parts = param.split("=");
            if (parts.length == 2) {
                final String paramKey = parts[0];
                final String paramVal = parts[1];
                parsedParams.put(paramKey, paramVal);
            } else {
                throw new ChannelSinkConnectorConfig.ConfigException(String.format("invalid param in channel params configuration %s", param));
            }
        }

        return parsedParams;
    }
}
