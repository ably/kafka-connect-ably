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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ChannelSinkTaskTest {
    private FakeAblyClient fakeAblyClient;
    private ChannelSinkTask SUT;

    private CountDownLatch countDownLatch;

    @BeforeEach
    public void setup() throws ChannelSinkConnectorConfig.ConfigException {
        final FakeClientFactory fakeClientFactory = new FakeClientFactory(1000, record -> countDownLatch.countDown());
        SUT = new ChannelSinkTask(fakeClientFactory);
        countDownLatch = new CountDownLatch(1000);
    }
    @AfterEach
    public void tearDown() throws ChannelSinkConnectorConfig.ConfigException {
        fakeAblyClient.stop();
        countDownLatch = null;
    }

    /**
     * Add records while changing suspended state and ensure records are processed in correct order
     * */
    @RepeatedTest(10)
    public void addRecordsChangingSuspensionStateRandomly_messagesReceivedInCorrectOrder(TestInfo testInfo) throws InterruptedException {
        SUT.start(null);
        fakeAblyClient = (FakeAblyClient) SUT.getAblyClient();
        fakeAblyClient.randomlyChangeSuspendedState();
        final Random random = new Random();
        int recordCounter = 0;
        for (int i = 0; i < 100; i++) {
            final List<SinkRecord> sinkRecords = new ArrayList<>();
            for (int j = 0; j < 10; j++) {
                final String recordValue = "record"+recordCounter++;
                SinkRecord sinkRecord = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, null, null, recordValue, 0);
                sinkRecords.add(sinkRecord);

            }
            //run for up to 100 ms each
            Thread.sleep(random.nextInt(100));
            SUT.put(sinkRecords);
        }
        countDownLatch.await(10, TimeUnit.SECONDS);
        final List<SinkRecord> publishedRecords = fakeAblyClient.getPublishedRecords();
        assertEquals(1000, publishedRecords.size());

        final int[] receiverCount = {0};
        publishedRecords.forEach(sinkRecord -> assertEquals("record"+(receiverCount[0]++), sinkRecord.value()));
    }

    /**
     * Ensures that records are published/queued appropriately before, during, and after Ably connection is suspended
     * */
    @Test
    public void whenPublishingAfterSuspension_messageIsPublishedSuccessfully(TestInfo testInfo) throws InterruptedException {
        SUT.start(null);
        fakeAblyClient = (FakeAblyClient) SUT.getAblyClient();

        // Publish a record
        final List<SinkRecord> sinkRecords = new ArrayList<>();
        SinkRecord sinkRecord = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, null, null, "record0", 0);
        sinkRecords.add(sinkRecord);
        Thread.sleep(1000);
        SUT.put(sinkRecords);

        // Check that the first record is published sucessfully
        final List<SinkRecord> publishedRecords = fakeAblyClient.getPublishedRecords();
        assertEquals(1, publishedRecords.size());
        assertEquals(publishedRecords.get(0).value(), "record0");

        // Enter the suspended state
        fakeAblyClient.setSuspendedState(true);
        
        // Publish while suspended
        final List<SinkRecord> sinkRecordsWhileSuspended = new ArrayList<>();
        SinkRecord sinkRecordWhileSuspended = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, null, null, "record1", 0);
        sinkRecordsWhileSuspended.add(sinkRecordWhileSuspended);
        Thread.sleep(1000);
        SUT.put(sinkRecordsWhileSuspended);

        // Check that the message is not published while suspended
        final List<SinkRecord> publishedRecordsWhileSuspended = fakeAblyClient.getPublishedRecords();
        assertEquals(1, publishedRecordsWhileSuspended.size());

        // Exit the suspended state
        fakeAblyClient.setSuspendedState(false);

        // Check that the record we published while suspended is now published successfully
        Thread.sleep(1000);
        final List<SinkRecord> publishedRecordsAfterReconnection = fakeAblyClient.getPublishedRecords();
        assertEquals(2, publishedRecordsAfterReconnection.size());
        assertEquals(publishedRecordsAfterReconnection.get(1).value(), "record1");

        // Publish another message
        final List<SinkRecord> sinkRecordsAfterSuspended = new ArrayList<>();
        SinkRecord sinkRecordAfterSuspended = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, null, null, "record2", 0);
        sinkRecordsAfterSuspended.add(sinkRecordAfterSuspended);
        Thread.sleep(1000);
        SUT.put(sinkRecordsAfterSuspended);

        // Check that the final message is published correctly
        final List<SinkRecord> publishedRecordsAfterSuspended = fakeAblyClient.getPublishedRecords();
        assertEquals(3, publishedRecordsAfterSuspended.size());
        assertEquals(publishedRecordsAfterSuspended.get(2).value(), "record2");
    }
}
