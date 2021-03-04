package io.ably.kakfa.connect;

import java.util.Collection;
import java.util.Map;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.ably.lib.realtime.AblyRealtime;
import io.ably.lib.realtime.Channel;
import io.ably.lib.types.AblyException;
import io.ably.lib.types.ClientOptions;
import io.ably.lib.types.Message;
import io.ably.lib.util.Log.LogHandler;

public class ChannelSinkTask extends SinkTask {
    private static Logger logger = LoggerFactory.getLogger(ChannelSinkTask.class);
    private static String[] severities = new String[]{"", "", "VERBOSE", "DEBUG", "INFO", "WARN", "ERROR", "ASSERT"};

    ChannelSinkConnectorConfig config;
    AblyRealtime ably;
    Channel channel;

    @Override
    public void start(Map<String, String> settings) {
        this.config = new ChannelSinkConnectorConfig(settings);

        if (this.config.clientOptions == null) {
            logger.error("Ably client options were not initialized due to invalid configuration.");
            return;
        }

        if (this.config.channelOptions == null) {
            logger.error("Ably channel options were not initialized due to invalid configuration.");
            return;
        }
        this.config.clientOptions.logHandler = new LogHandler() {
            public void println(int severity, String tag, String msg, Throwable tr) {
                if (severity < 0 || severity >= severities.length) {
                    severity = 3;
                }
                switch (severities[severity]) {
                    case "VERBOSE":
                        ChannelSinkTask.logger.trace(msg, tr);
                        break;
                    case "DEBUG":
                        ChannelSinkTask.logger.debug(msg, tr);
                        break;
                    case "INFO":
                        ChannelSinkTask.logger.info(msg, tr);
                        break;
                    case "WARN":
                        ChannelSinkTask.logger.warn(msg, tr);
                        break;
                    case "ERROR":
                        ChannelSinkTask.logger.error(msg, tr);
                        break;
                    case "default":
                        if (ChannelSinkTask.logger.isDebugEnabled()) {
                            ChannelSinkTask.logger.debug(
                                String.format("severity: %d, tag: %s, msg: %s, err"),
                                severity, tag, msg, (tr != null) ? tr.getMessage() : "null"
                            );
                        }
                }
            }
        };

        try {
            this.ably = new AblyRealtime(this.config.clientOptions);
            this.channel = this.ably.channels.get(this.config.channel, this.config.channelOptions);
        } catch(AblyException e) {
            ChannelSinkTask.logger.error("error initializing ably client", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (this.channel == null) {
            // Put is not retryable, throwing error will indicate this
            throw new ConnectException("ably client is uninitialized");
        }

        for (SinkRecord r : records) {
            // TODO: add configuration to change the event name
            Message message = new Message("sink", r.value()); // TODO: read and encode the record
            message.id = String.format("%d:%d:%d", r.topic().hashCode(), r.kafkaPartition(), r.kafkaOffset());
            try {
                this.channel.publish(message);
            } catch (AblyException e) {
                // The ably client should attempt retries itself, so if we do have to handle an exception here,
                // we can assume that it is not retryably.
                throw new ConnectException("ably client failed to publish", e);
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        // Currently irrelevant because the put call is synchronous
        return;
    }

    @Override
    public void stop() {
        if (this.ably != null) {
            this.ably.close();
            this.ably = null;
            this.channel = null;
        }
    }

    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }
}
