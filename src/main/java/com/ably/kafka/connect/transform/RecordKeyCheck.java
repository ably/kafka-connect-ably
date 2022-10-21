package com.ably.kafka.connect.transform;

import com.ably.kafka.connect.config.ConfigValueEvaluator;
import com.ably.kafka.connect.utils.ByteArrayUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static com.ably.kafka.connect.config.ConfigValueEvaluator.KEY_TOKEN;

public class RecordKeyCheck<R extends ConnectRecord<R>> implements Transformation<R> {
    private final ConfigValueEvaluator configValueEvaluator = new ConfigValueEvaluator();
    private final String channelConfig;
    private final String messageNameConfig;

    public RecordKeyCheck(String channelConfig, String messageNameConfig) {
        this.channelConfig = channelConfig;
        this.messageNameConfig = messageNameConfig;
    }

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
    }
}
