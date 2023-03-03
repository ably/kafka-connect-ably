package com.ably.kafka.connect.fakes;

import io.ably.lib.realtime.ConnectionState;
import java.util.Random;

//randomly changes state
class RandomConnectionStateChanger implements Runnable {
    private ConnectionState lastState;

    interface Callback {
        void onStateChange(ConnectionState state);
    }

    private final long randomTimeBound;
    private final Callback callback;
    private final Random random;

    RandomConnectionStateChanger(long randomTimeBound, Callback callback) {
        this.randomTimeBound = randomTimeBound;
        this.callback = callback;
        random = new Random();
    }

    @Override
    public void run() {
        while (true) {
            if (lastState == null || lastState == ConnectionState.suspended) {
                callback.onStateChange(ConnectionState.connected);
                lastState = ConnectionState.connected;
            } else {
                callback.onStateChange(ConnectionState.suspended);
                lastState = ConnectionState.suspended;
            }

            final long sleepTime = random.nextLong() % randomTimeBound;
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
