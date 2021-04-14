/**
 * Copyright © 2021 Ably Real-time Ltd. (support@ably.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.ably.kakfa.connect;

import java.util.Base64;
import java.util.Collection;
import java.util.Map;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.ably.lib.realtime.AblyRealtime;
import io.ably.lib.realtime.Channel;
import io.ably.lib.types.AblyException;
import io.ably.lib.types.Message;
import io.ably.lib.types.MessageExtras;
import io.ably.lib.util.JsonUtils;
import io.ably.lib.util.JsonUtils.JsonUtilsObject;
import io.ably.lib.util.Log.LogHandler;

public class ChannelSinkTask extends SinkTask {
    private static Logger logger = LoggerFactory.getLogger(ChannelSinkTask.class);
    private static String[] severities = new String[]{"", "", "VERBOSE", "DEBUG", "INFO", "WARN", "ERROR", "ASSERT"};

    ChannelSinkConnectorConfig config;
    AblyRealtime ably;
    Channel channel;

    @Override
    public void start(Map<String, String> settings) {
        logger.info("Starting Ably channel Sink task");

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
            try {
                Message message = new Message("sink", r.value());
                message.id = String.format("%d:%d:%d", r.topic().hashCode(), r.kafkaPartition(), r.kafkaOffset());

                JsonUtilsObject kafkaExtras = this.kafkaExtras(r);
                if(kafkaExtras.toJson().size() > 0 ) {
                    message.extras = new MessageExtras(JsonUtils.object().add("kafka", kafkaExtras).toJson());
                }

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
        logger.info("Stopping Ably channel Sink task");

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

    /**
     * Returns the Kafka extras object to use when converting a Kafka message
     * to an Ably message.
     *
     * If the Kafka message has a key, it is base64 encoded and set as the
     * "key" field in the extras.
     *
     * If the Kafka message has headers, they are set as the "headers" field
     * in the extras.
     *
     * @param record The sink record representing the Kafka message
     * @return       The Kafka message extras object
     */
    private JsonUtilsObject kafkaExtras(SinkRecord r) {
        JsonUtilsObject extras = JsonUtils.object();

        byte[] key = (byte[])r.key();
        if(key != null) {
            extras.add("key", Base64.getEncoder().encodeToString(key));
        }

        if(!r.headers().isEmpty()) {
            JsonUtilsObject headers = JsonUtils.object();
            for (Header header : r.headers()) {
                headers.add(header.key(), header.value());
            }
            extras.add("headers", headers);
        }

        return extras;
    }
}
