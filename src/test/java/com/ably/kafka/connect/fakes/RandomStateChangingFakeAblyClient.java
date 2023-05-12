package com.ably.kafka.connect.fakes;

import com.ably.kafka.connect.client.AblyClient;
import com.ably.kafka.connect.client.SuspensionCallback;
import io.ably.lib.realtime.ConnectionState;
import io.ably.lib.types.AblyException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.List;

public class RandomStateChangingFakeAblyClient implements AblyClient {
    public interface Listener {
        void onPublish(SinkRecord record);
    }

    final long randomTimeBound;
    private final List<SinkRecord> publishedRecords = new ArrayList<>();
    private RandomConnectionStateChanger randomConnectionStateChanger;
    private final Listener publishListener;

    public RandomStateChangingFakeAblyClient(long randomTimeBound, Listener publishListener) {
        this.randomTimeBound = randomTimeBound;
        this.publishListener = publishListener;
    }
    public void connect(SuspensionCallback suspensionCallback) throws ConnectException {
       randomConnectionStateChanger = new RandomConnectionStateChanger(randomTimeBound, state -> {
           if (state == ConnectionState.connected){
               suspensionCallback.onSuspendedStateChange(false);
           } else if (state == ConnectionState.suspended) {
               suspensionCallback.onSuspendedStateChange(true);
           }
       });
       new Thread(randomConnectionStateChanger).start();
    }

    @Override
    public void connect() throws ConnectException, AblyException {

    }

    @Override
    public void publishFrom(SinkRecord record) throws ConnectException {
        publishedRecords.add(record);
        publishListener.onPublish(record);
    }

    @Override
    public void publishBatch(List<SinkRecord> records) throws ConnectException, AblyException {

    }

    public List<SinkRecord> getPublishedRecords() {
        return publishedRecords;
    }

    @Override
    public void stop() {
        publishedRecords.clear();
    }

}
