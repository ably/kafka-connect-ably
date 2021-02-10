package io.ably.kakfa.connect;

import java.util.Collection;
import java.util.Map;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
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
        ClientOptions opts = new ClientOptions(); // TODO: get client options from settings
        opts.logHandler = new LogHandler() {
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

        this.config = new ChannelSinkConnectorConfig(settings);
        try {
            this.ably = new AblyRealtime(new ClientOptions());
        } catch(AblyException e) {
            ChannelSinkTask.logger.error("error initializing ably client", e);
        }

        this.channel = ably.channels.get("test");
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (this.channel == null) {
            // Put is not retryable, throwing error will indicate this
            throw new org.apache.kafka.connect.errors.ConnectException("ably client is uninitialized");
        }

        // TODO: queue the collection on the channel, mark as retryable if the queue is
        // full
        Message[] messages = new Message[records.size()];
        int index = 0;
        for (SinkRecord r : records) {
            messages[index++] = new Message("put", "todo - read the record to binary"); // TODO: read and encode the record
            logger.debug(String.format("key: %s, value: %s", r.key().toString(), r.value().toString()));
        }

        try {
            this.channel.publish(messages);
        } catch (AblyException e) {
            // Assume that this isn't retryable but we should figure out if
            // ably client exceptions are retryable and which ones are if they
            // are
            throw new org.apache.kafka.connect.errors.ConnectException("ably client failed to publish");
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        // Currently irrelevant because the put call is synchronous
    }

    @Override
    public void stop() {
        // Close resources here.
        if (this.ably != null) {
            this.ably.close();
            this.ably = null;
        }
    }

    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }
}
