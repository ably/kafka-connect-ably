package com.ably.kafka.connect.batch;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Consumer;

/**
 * A buffer that will automatically flush content using the provided flush operation
 * when either its size limit is reached or the elapsed time since the last flush
 * expires.
 *
 * @param <T> The type of the items being buffered.
 */
public class AutoFlushingBuffer<T> {

    private final long maxAgeMillis;
    private final int maxSize;
    private final Consumer<List<T>> flushOperation;
    private final List<T> buffer;
    private Timer flushTimer = null;


    /**
     * Construct a new automatically flushing buffer of given capacity and expiry.
     *
     * @param maxAgeMillis Maximum time to leave items in the buffer before flushing.
     * @param maxSize Maximum amount of items to buffer before triggering a flush.
     * @param flushOperation The flush callback operation. Will be passed a new list containing
     *                       the completed buffer, which can be mutated freely from here on.
     */
    public AutoFlushingBuffer(long maxAgeMillis, int maxSize, Consumer<List<T>> flushOperation) {
        this.maxAgeMillis = maxAgeMillis;
        this.maxSize = maxSize;
        this.flushOperation = flushOperation;
        this.buffer = new ArrayList<>(maxSize);
    }

    /**
     * Copy buffer content out to a new list and pass it to the flush operation for consumption.
     * Also clears the timer, if it's currently running. Can be safely called from within a timer
     * callback.
     */
    private synchronized void flush() {
        // Cancel the timer, if it's running
        if (flushTimer != null)  {
            this.flushTimer.cancel();
            this.flushTimer = null;
        }

        // Copy buffer to a new list, reset and invoke flush operation
        final List<T> flushedItems = new ArrayList<>(this.buffer);
        this.buffer.clear();
        this.flushOperation.accept(flushedItems);
    }

    /**
     * If the flush timer is not already running, start the timer to expire after maxAgeMillis
     * and call flush() if it expires.
     */
    private void startFlushTimer() {
        // Only start the timer if it's not already running
        if (this.flushTimer == null) {
            this.flushTimer = new Timer(AutoFlushingBuffer.class.getName());
            this.flushTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    flush();
                }
            }, this.maxAgeMillis);
        }
    }

    /**
     * Buffer the given items until the buffer is full or the timer expires.
     * Note that this may or may not trigger one or more flushes. It's possible
     * to trigger multiple flushes if given items list is many times longer than the
     * buffer capacity.
     *
     * @param items List of items to buffer.
     */
    public synchronized void addAll(List<T> items) {
        // Consume from items, flushing each time we fill the buffer
        while (items.size() > 0) {
            final int remainingSpace = this.maxSize - this.buffer.size();
            final int itemsAdded = Math.min(remainingSpace, items.size());
            buffer.addAll(items.subList(0, itemsAdded));
            if (buffer.size() >= this.maxSize) {
                flush();
            }
            items = items.subList(itemsAdded, items.size());
        }

        // If we now have buffered items, ensure the flush timer is running
        if (this.buffer.size() > 0) {
            startFlushTimer();
        }
    }
}
