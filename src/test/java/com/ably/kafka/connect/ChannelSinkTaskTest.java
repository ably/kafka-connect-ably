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

import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.fakes.FakeAblyClient;
import com.ably.kafka.connect.fakes.FakeClientFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ChannelSinkTaskTest {
    private FakeAblyClient fakeAblyClient;
    private ChannelSinkTask SUT;

    @BeforeEach
    public void setup() throws ChannelSinkConnectorConfig.ConfigException {
        final FakeClientFactory fakeClientFactory = new FakeClientFactory(1000);
        SUT = new ChannelSinkTask(fakeClientFactory);
    }
    @AfterEach
    public void tearDown() throws ChannelSinkConnectorConfig.ConfigException {
        fakeAblyClient.stop();
    }

    /**
     * Add records while changing suspended state and ensure records are processed in correct order
     * */
    @RepeatedTest(10)
    public void addRecordsChangingSuspensionStateRandomly_messagesReceivedInCorrectOrder(TestInfo testInfo) throws InterruptedException {
        SUT.start(null);
        fakeAblyClient = (FakeAblyClient) SUT.getAblyClient();
        final Random random = new Random();
        int recordCounter = 0;
        for (int i = 0; i < 100; i++) {
            final List<SinkRecord> sinkRecords = new ArrayList<>();
            for (int j = 0; j < 10; j++) {
                final String recordValue = "record"+recordCounter++;
                SinkRecord sinkRecord = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, null, null, recordValue, 0);
                sinkRecords.add(sinkRecord);

            }
            //run for up to 10 seconds each
            Thread.sleep(random.nextInt(100));
            SUT.put(sinkRecords);
        }

        final List<SinkRecord> publishedRecords = fakeAblyClient.getPublishedRecords();
        assertEquals(1000, publishedRecords.size());

        final int[] receiverCount = {0};
        publishedRecords.forEach(sinkRecord -> assertEquals("record"+(receiverCount[0]++), sinkRecord.value()));
    }

}
