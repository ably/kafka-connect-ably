package com.ably.kafka.connect.client;

import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.offset.OffsetRegistry;
import com.ably.kafka.connect.utils.ClientOptionsLogHandler;
import io.ably.lib.types.AblyException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;

public class DefaultAblyClientFactory implements AblyClientFactory {
    private static final Logger logger = LoggerFactory.getLogger(DefaultAblyClientFactory.class);

    @Nullable private final ErrantRecordReporter dlqReporter;
    private final OffsetRegistry offsetRegistryService;

    public DefaultAblyClientFactory(
        @Nullable ErrantRecordReporter dlqReporter,
        OffsetRegistry offsetRegistryService) {
        this.dlqReporter = dlqReporter;
        this.offsetRegistryService = offsetRegistryService;
    }

    @Override
    public DefaultAblyBatchClient create(Map<String, String> settings) throws AblyException, ChannelSinkConnectorConfig.ConfigException {
        final ChannelSinkConnectorConfig connectorConfig = new ChannelSinkConnectorConfig(settings);
        if (connectorConfig.clientOptions == null) {
            throw new ConfigException("Ably client options were not initialized due to invalid configuration.");
        }
        connectorConfig.clientOptions.logHandler = new ClientOptionsLogHandler(logger);

        return new DefaultAblyBatchClient(connectorConfig, dlqReporter, offsetRegistryService);
    }
}
