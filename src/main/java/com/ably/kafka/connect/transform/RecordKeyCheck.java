package com.ably.kafka.connect.transform;

import com.ably.kafka.connect.utils.ByteArrayUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.nio.charset.StandardCharsets;
import java.util.Map;


public class RecordKeyCheck<R extends ConnectRecord<R>> implements Transformation<R> {
    private final static String KEY_TOKEN = "#{key}";
    private final static String CHANNEL_CONFIG = "channel.name";
    private final static String MESSAGE_CONFIG = "message.name";
    public static final ConfigDef CONFIG_DEF;
    private String channelConfig;
    private String messageNameConfig;

    @Override
    public R apply(R record) {
        final byte[] key = (byte[]) record.key();
        if (key == null) {
            return null;
        }
        String keyString = null;
        if (ByteArrayUtils.isUTF8Encoded(key)) {
            keyString = new String(key, StandardCharsets.UTF_8);
        }
        if (keyString == null && (channelConfig.contains(KEY_TOKEN) || messageNameConfig.contains(KEY_TOKEN))) {
            throw new IllegalArgumentException("Key is null or not a string type but pattern contains #{key}");
            //This SMT shouldn't be set if skippable is true - so we can throw an exception here
        }
        return record;
    }

    //not relevant for this transform
    @Override
    public ConfigDef config() {
        return null;
    }

    //not relevant for this transform
    @Override
    public void close() {
    }

    //not relevant for this transform
    @Override
    public void configure(Map<String, ?> map) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, map);
        this.channelConfig = config.getString(CHANNEL_CONFIG);
        this.messageNameConfig = config.getString(MESSAGE_CONFIG);
    }

    static {
        CONFIG_DEF = new ConfigDef().
            define(CHANNEL_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                null,
                ConfigDef.Importance.HIGH,
                "The channel name to publish to")
            .define(MESSAGE_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new NonEmptyListValidator(),
                ConfigDef.Importance.LOW,
                "The message name to publish");
    }
}
