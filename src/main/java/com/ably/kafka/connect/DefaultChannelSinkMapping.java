package com.ably.kafka.connect;

import io.ably.lib.realtime.AblyRealtime;
import io.ably.lib.realtime.Channel;
import io.ably.lib.types.AblyException;
import io.ably.lib.types.ChannelMode;
import io.ably.lib.types.ChannelOptions;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ably.kafka.connect.ChannelSinkConnectorConfig.*;

public class DefaultChannelSinkMapping implements ChannelSinkMapping {
    private final ChannelSinkConnectorConfig sinkConnectorConfig;
    private final ConfigValueEvaluator configValueEvaluator;

    public DefaultChannelSinkMapping(@Nonnull ChannelSinkConnectorConfig config, ConfigValueEvaluator configValueEvaluator) {
        sinkConnectorConfig = config;
        this.configValueEvaluator = configValueEvaluator;
    }

    @Override
    public Channel getChannel(@Nonnull SinkRecord sinkRecord, @Nonnull AblyRealtime ablyRealtime) throws AblyException,
            ChannelSinkConnectorConfig.ConfigException {
        return ablyRealtime.channels.get(getAblyChannelName(sinkRecord), getAblyChannelOptions(sinkRecord));
    }

    private String getAblyChannelName(SinkRecord sinkRecord) {
        return configValueEvaluator.evaluate(sinkRecord, sinkConnectorConfig.getString(CHANNEL_CONFIG));
    }

    private ChannelOptions getAblyChannelOptions(SinkRecord record) throws ChannelSinkConnectorConfig.ConfigException {
        final Logger logger = LoggerFactory.getLogger(ChannelSinkConnectorConfig.class);
        ChannelOptions opts;
        String cipherKey = sinkConnectorConfig.getString(CLIENT_CHANNEL_CIPHER_KEY);

        if (cipherKey != null && !cipherKey.trim().isEmpty()) {
            try {
                opts = ChannelOptions.withCipherKey(cipherKey);
            } catch (AblyException e) {
                logger.error("Error configuring channel cipher key", e);
                throw new ChannelSinkConnectorConfig.ConfigException("Error configuring channel cipher key", e);
            }
        } else {
            opts = new ChannelOptions();
        }

        // Since we're only publishing, set the channel mode to publish only
        opts.modes = new ChannelMode[]{ChannelMode.publish};
        opts.params = getChannelParams(record, sinkConnectorConfig.getList(CLIENT_CHANNEL_PARAMS));
        return opts;
    }

    private Map<String, String> getChannelParams(SinkRecord sinkRecord, List<String> params) throws ChannelSinkConnectorConfig.ConfigException {
        final Logger logger = LoggerFactory.getLogger(ChannelSinkConnectorConfig.class);

        Map<String, String> parsedParams = new HashMap<String, String>();
        for (String param : params) {
            String[] parts = param.split("=");
            if (parts.length == 2) {
                final String paramKey = parts[0];
                final String paramVal = parts[1];
                parsedParams.put(paramKey, configValueEvaluator.evaluate(sinkRecord, paramVal));
            } else {
                ChannelSinkConnectorConfig.ConfigException e = new ChannelSinkConnectorConfig.ConfigException(String.format("invalid param string %s", param));
                logger.error("invalid param in channel params configuration", e);
                throw e;
            }
        }

        return parsedParams;
    }
}
