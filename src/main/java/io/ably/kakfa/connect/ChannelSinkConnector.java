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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationImportant;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationNote;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip;
import com.github.jcustenborder.kafka.connect.utils.config.Title;

/**
 *
 */

@Description("This is a description of this connector and will show up in the documentation")
@DocumentationImportant("This is a important information that will show up in the documentation.")
@DocumentationTip("This is a tip that will show up in the documentation.")
@Title("Super Sink Connector") //This is the display name that will show up in the documentation.
@DocumentationNote("This is a note that will show up in the documentation")
public class ChannelSinkConnector extends SinkConnector {

    /*
     * Your connector should never use System.out for logging. All of your classes should use slf4j
     * for logging
     */
    private static Logger log = LoggerFactory.getLogger(ChannelSinkConnector.class);
    private ChannelSinkConnectorConfig config;

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        //TODO: Define the individual task configurations that will be executed.

        /**
         * This is used to schedule the number of tasks that will be running. This should not exceed maxTasks.
         * Here is a spot where you can dish out work. For example if you are reading from multiple tables
         * in a database, you can assign a table per task.
         */

        HashMap<String, String> stringConfig = new HashMap<>();

        for (Map.Entry<String, ?> entry : config.values().entrySet()) {
            if (entry.getValue() instanceof String) {
                stringConfig.put(entry.getKey(), (String)entry.getValue());
            }
        }

        ArrayList<Map<String, String>> configs = new ArrayList<>();
        configs.add(stringConfig);

        return configs;
    }

    @Override
    public void start(Map<String, String> settings) {
        config = new ChannelSinkConnectorConfig(settings);



        //TODO: initialize the shared ably client

        /**
         * This will be executed once per connector. This can be used to handle connector level setup. For
         * example if you are persisting state, you can use this to method to create your state table. You
         * could also use this to verify permissions
         */

    }

    @Override
    public void stop() {
        //TODO: close the ably client
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
