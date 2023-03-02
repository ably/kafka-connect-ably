package com.ably.kafka.connect;
import java.util.ArrayList;
import java.util.List;

/**
Thishis class holds records that are failed to publish after the Ably connection went into suspended state.
 It will act as an intermediate queue to publish messages rejected between suspension and reconnection
* */
public class SuspendQueue<T> {
    private final List<T> queue;

    SuspendQueue() {
        queue = new ArrayList<>();
    }

    synchronized void enqueue(T t) {
        queue.add(t);
    }

    synchronized T dequeue() {
        return queue.isEmpty() ? null : queue.remove(0);
    }
}
