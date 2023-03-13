package com.ably.kafka.connect.fakes;

import com.ably.kafka.connect.client.AblyClient;
import com.ably.kafka.connect.client.SuspensionCallback;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.List;

public class ManualStateChangingFakeAblyClient implements AblyClient {
    private final List<SinkRecord> publishedRecords = new ArrayList<>();
    private SuspensionCallback suspensionCallback;


    @Override
    public void connect(SuspensionCallback suspensionCallback) throws ConnectException {
       this.suspensionCallback = suspensionCallback;
    }

    @Override
    public void publishFrom(SinkRecord record) throws ConnectException {
        publishedRecords.add(record);
    }

    public List<SinkRecord> getPublishedRecords() {
        return publishedRecords;
    }

    @Override
    public void stop() {
        publishedRecords.clear();
    }

    public void setSuspendedState(boolean state) {
        this.suspensionCallback.onSuspendedStateChange(state);
    }

}
