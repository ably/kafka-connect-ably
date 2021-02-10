package io.ably.kakfa.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;

import java.util.Map;


public class ChannelSinkConnectorConfig extends AbstractConfig {

  public static final String CHANNEL_CONFIG = "channel";
  private static final String CHANNEL_CONFIG_DOC = "The ably channel name to use for publishing.";
  private static final String TOPIC_CONFIG = "topic";
  private static final String TOPIC_CONFIG_DOC = "The kafka topic to read from.";

  public final String channel;
  public final String topic;

  public ChannelSinkConnectorConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.channel = this.getString(CHANNEL_CONFIG);
    this.topic = this.getString(TOPIC_CONFIG);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(
            ConfigKeyBuilder.of(CHANNEL_CONFIG, Type.STRING)
                .documentation(CHANNEL_CONFIG_DOC)
                .importance(Importance.HIGH)
                .build()
        )
        .define(
          ConfigKeyBuilder.of(TOPIC_CONFIG, Type.STRING)
            .documentation(TOPIC_CONFIG_DOC)
            .importance(Importance.HIGH)
            .build()
        );
  }
}
