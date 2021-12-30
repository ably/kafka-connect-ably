/**
 * Copyright © 2021 Ably Real-time Ltd. (support@ably.com)
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

package com.ably.kafka.connect;

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
    private static final Logger logger = LoggerFactory.getLogger(ChannelSinkTask.class);
    private static final String[] severities = new String[]{"", "", "VERBOSE", "DEBUG", "INFO", "WARN", "ERROR", "ASSERT"};

    ChannelSinkConnectorConfig config;
    AblyRealtime ably;
    Channel channel;

    @Override
    public void start(Map<String, String> settings) {
        logger.info("Starting Ably channel Sink task");

        config = new ChannelSinkConnectorConfig(settings);

        if (config.clientOptions == null) {
            logger.error("Ably client options were not initialized due to invalid configuration.");
            return;
        }

        if (config.channelOptions == null) {
            logger.error("Ably channel options were not initialized due to invalid configuration.");
            return;
        }
        config.clientOptions.logHandler = (severity, tag, msg, tr) -> {
            if (severity < 0 || severity >= severities.length) {
                severity = 3;
            }
            switch (severities[severity]) {
                case "VERBOSE":
                    logger.trace(msg, tr);
                    break;
                case "DEBUG":
                    logger.debug(msg, tr);
                    break;
                case "INFO":
                    logger.info(msg, tr);
                    break;
                case "WARN":
                    logger.warn(msg, tr);
                    break;
                case "ERROR":
                    logger.error(msg, tr);
                    break;
                case "default":
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                            String.format(
                                "severity: %d, tag: %s, msg: %s, err",
                                severity, tag, msg, (tr != null) ? tr.getMessage() : "null"
                            )
                        );
                    }
            }
        };

        try {
            ably = new AblyRealtime(config.clientOptions);
            channel = ably.channels.get(config.channelName, config.channelOptions);
        } catch(AblyException e) {
            logger.error("error initializing ably client", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (channel == null) {
            // Put is not retryable, throwing error will indicate this
            throw new ConnectException("ably client is uninitialized");
        }

        for (SinkRecord r : records) {
            // TODO: add configuration to change the event name
            try {
                Message message = new Message("sink", r.value());
                message.id = String.format("%d:%d:%d", r.topic().hashCode(), r.kafkaPartition(), r.kafkaOffset());

                JsonUtilsObject kafkaExtras = createKafkaExtras(r);
                if(kafkaExtras.toJson().size() > 0 ) {
                    message.extras = new MessageExtras(JsonUtils.object().add("kafka", kafkaExtras).toJson());
                }

                channel.publish(message);
            } catch (AblyException e) {
                logger.error("Failed to publish message", e);
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

        if (ably != null) {
            ably.close();
            ably = null;
            channel = null;
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
    private JsonUtilsObject createKafkaExtras(SinkRecord record) {
        JsonUtilsObject extras = JsonUtils.object();

        byte[] key = (byte[])record.key();
        if(key != null) {
            extras.add("key", Base64.getEncoder().encodeToString(key));
        }

        if(!record.headers().isEmpty()) {
            JsonUtilsObject headers = JsonUtils.object();
            for (Header header : record.headers()) {
                headers.add(header.key(), header.value());
            }
            extras.add("headers", headers);
        }

        return extras;
    }
}
