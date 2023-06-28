package com.ably.kafka.connect;

import com.ably.kafka.connect.batch.AutoFlushingBuffer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link com.ably.kafka.connect.batch.AutoFlushingBuffer}
 */
public class AutoFlushingBufferTest {

    // For tests that are not interested in the async timer
    private static final int NEVER_EXPIRE = Integer.MAX_VALUE;

    /**
     * Check that the buffer will flush content when maxSize is reached, but
     * not before.
     */
    @Test
    public void testFlushesWhenFull() {
        final List<Integer> captured = new ArrayList<>(5);
        final AutoFlushingBuffer<Integer> sut = new AutoFlushingBuffer<>(NEVER_EXPIRE, 5, captured::addAll);

        // Should do nothing
        sut.addAll(Collections.emptyList());
        assertEquals(Collections.emptyList(), captured);

        // Should still capture nothing
        sut.addAll(List.of(1, 2, 3, 4));
        assertEquals(Collections.emptyList(), captured);

        // Should now flush
        sut.addAll(List.of(5));
        assertEquals(List.of(1, 2, 3, 4, 5), captured);
    }

    /**
     * Ensure that additional items that don't fit in the current buffer capacity
     * are spilled over to the next chunk
     */
    @Test
    public void testSpillsOverAdditionalItems() {
        final List<Integer> captured = new ArrayList<>(3);
        final AutoFlushingBuffer<Integer> sut = new AutoFlushingBuffer<>(NEVER_EXPIRE, 3, captured::addAll);

        // Should flush the first 3 (== maxSize) items only
        sut.addAll(List.of(1, 2, 3, 4));
        assertEquals(List.of(1, 2, 3), captured);

        // 4th element should have been spilled over
        sut.addAll(List.of(5, 6));
        assertEquals(List.of(1, 2, 3, 4, 5, 6), captured);
    }

    /**
     * Test that we can handle batches of items larger than the buffer capacity by
     * triggering multiple flushes if needed.
     */
    @Test
    public void testAddMoreItemsThanMaxSize() {
        final List<List<Integer>> captured = new ArrayList<>();
        final AutoFlushingBuffer<Integer> sut = new AutoFlushingBuffer<>(NEVER_EXPIRE, 3, captured::add);

        // Should flush twice from single call
        sut.addAll(List.of(1, 2, 3, 4, 5, 6));
        assertEquals(List.of(
            List.of(1, 2, 3),
            List.of(4, 5, 6)
        ), captured);
    }

    /**
     * Ensure that incomplete buffers are flushed when the timer expires
     *
     * @throws InterruptedException if test interrupted while waiting for timer
     */
    @Test
    public void testFlushesContentOnExpiry() throws InterruptedException {
        final List<Integer> captured = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final AutoFlushingBuffer<Integer> sut = new AutoFlushingBuffer<>(100, 5, flushed -> {
            captured.addAll(flushed);
            latch.countDown();
        });

        // Won't trigger flush immediately, as we have capacity for two more
        sut.addAll(List.of(1, 2, 3));
        assertEquals(Collections.emptyList(), captured);

        // Wait for flush
        final boolean flushed = latch.await(250, TimeUnit.MILLISECONDS);
        assertTrue(flushed);
        assertEquals(List.of(1, 2, 3), captured);
    }

    /**
     * Check that spilled over content will automatically be flushed after the expiry time.
     *
     * @throws InterruptedException if test interrupted while waiting for timer.
     */
    @Test
    public void testTimerResetsAfterFlush() throws InterruptedException {
        final List<List<Integer>> captured = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(2);
        final AutoFlushingBuffer<Integer> sut = new AutoFlushingBuffer<>(100, 3, flushed -> {
            captured.add(flushed);
            latch.countDown();
        });

        // Should trigger an initial flush
        sut.addAll(List.of(1, 2, 3, 4));
        assertEquals(List.of(
            List.of(1, 2, 3)
        ), captured);

        // 4th element should be flushed when timer expires
        final boolean flushed = latch.await(250, TimeUnit.MILLISECONDS);
        assertTrue(flushed);
        assertEquals(List.of(
            List.of(1, 2, 3),
            List.of(4)
        ), captured);
    }

    /**
     * Ensure that the flush timer isn't idly polling the flush action while there
     * is no content in the buffer to be flushed.
     *
     * @throws InterruptedException if test interrupted while waiting for flush timer
     */
    @Test
    public void testNoIdlePolling() throws InterruptedException {
        final List<Integer> captured = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final AutoFlushingBuffer<Integer> sut = new AutoFlushingBuffer<>(100, 3, flushed -> {
            captured.addAll(flushed);
            latch.countDown();
        });

        // Should not start the flush timer
        sut.addAll(Collections.emptyList());

        final boolean flushed = latch.await(250, TimeUnit.MILLISECONDS);
        assertFalse(flushed);
        assertEquals(Collections.emptyList(), captured);
    }
}
