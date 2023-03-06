package com.ably.kafka.connect.fakes;

import com.ably.kafka.connect.client.AblyClient;
import com.ably.kafka.connect.client.SuspensionCallback;
import io.ably.lib.realtime.ConnectionState;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.List;

public class FakeAblyClient implements AblyClient {
    final long randomTimeBound;
    private final List<SinkRecord> publishedRecords = new ArrayList<>();
    private SuspensionCallback suspensionCallback;

    public FakeAblyClient(long randomTimeBound) {
        this.randomTimeBound = randomTimeBound;
    }

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

    public void randomlyChangeSuspendedState() {
       new RandomConnectionStateChanger(randomTimeBound, state -> {
           if (state == ConnectionState.connected){
               suspensionCallback.onSuspendedStateChange(false);
           } else if (state == ConnectionState.suspended) {
               suspensionCallback.onSuspendedStateChange(true);
           }
       });
    }

    public void setSuspendedState(boolean state) {
        this.suspensionCallback.onSuspendedStateChange(state);
    }

    @Override
    public void stop() {
       publishedRecords.clear();
    }

}
