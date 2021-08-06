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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.Title;

@Title("Channel Sink Connector")
@Description("Publishes a kafka topic to an ably channel")
public class ChannelSinkConnector extends SinkConnector {
    private static final Logger logger = LoggerFactory.getLogger(ChannelSinkConnector.class);
    private Map<String, String> settings;

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        //TODO: Add multi-partition publisher
        return Collections.singletonList(this.settings);
    }

    @Override
    public void start(Map<String, String> settings) {
        logger.info("Starting Ably channel Sink connector");
        this.settings = settings;
    }

    @Override
    public void stop() {
        logger.info("Stopping Ably channel Sink connector");
    }

    @Override
    public ConfigDef config() {
        return ChannelSinkConnectorConfig.config();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ChannelSinkTask.class;
    }

    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }
}
