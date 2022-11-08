package com.ably.kafka.connect.transform;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/*
 * This transformation is used to check the validitiy of the key of the record and is strictly to use with the Ably sink connector.
 * This transformation is to be used only when one of the configurations given contains #{key} in them.
 **/
public class RecordKeyCheck<R extends ConnectRecord<R>> implements Transformation<R> {
    private final static String KEY_TOKEN = "#{key}";
    private final static String CHANNEL_CONFIG = "channel.name";
    private final static String MESSAGE_CONFIG = "message.name";
    public static final ConfigDef CONFIG_DEF;
    private String channelConfig;
    private String messageNameConfig;

    @Override
    public R apply(R record) {
        final String keyString = ByteArrayUtils.utf8String((byte[]) record.key());
        if (keyString == null && (channelConfig.contains(KEY_TOKEN) || messageNameConfig.contains(KEY_TOKEN))) {
            System.out.println(this.getClass().getSimpleName()+": Key is null or not a string type but pattern contains #{key}");
            throw new IllegalArgumentException("Key is null or not a string type but pattern contains #{key}");
        }
        return record;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> map) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, map);

        this.channelConfig = config.getString(CHANNEL_CONFIG);
        this.messageNameConfig = config.getString(MESSAGE_CONFIG);

        if (this.channelConfig == null && this.messageNameConfig == null) {
            throw new ConfigException("You must provide at least a configuration for this SMT");
        }

        //it's useless if the channel name doesn't contain the key token
        if (this.channelConfig  != null && !this.channelConfig.contains(KEY_TOKEN)) {
            throw new ConfigException("Channel name must contain #{key} token");
        }
        //Also for message name
        if (this.messageNameConfig != null && !messageNameConfig.contains(KEY_TOKEN)) {
            throw new ConfigException("Message name must contain #{key} token");
        }

    }

    static {
        CONFIG_DEF = new ConfigDef().
            define(CHANNEL_CONFIG,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.HIGH,
                "The channel name to publish to")
            .define(MESSAGE_CONFIG,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.LOW,
                "The message name to publish");
    }
}
