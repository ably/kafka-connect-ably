package com.ably.kafka.connect;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SuspendQueueTest {
    private SuspendQueue<String> suspendQueue = new SuspendQueue<>();

    @AfterEach
    void afterEach(){
       suspendQueue.clear();
    }

    @Test
    public void testAddRemoveItemsReceivedInCorrectOrderWhenAddedSequentially(){
        for (int i = 0; i < 20; i++) {
            suspendQueue.enqueue("item:"+i);
        }
        for (int i = 0; i < 20; i++) {
            assertEquals("item:"+i, suspendQueue.dequeue());
        }
        assertNull(suspendQueue.dequeue());
    }

 /*   add to the queue concurrently and read sequentially ensuring the right amount of items are received and also make
    sure that all items were receieved*/
    @Test
    public void testAddRemoveItemsReceivedInCorrectSizeWhenAddedConcurrently() throws InterruptedException {
        final Set<String> addSet = ConcurrentHashMap.newKeySet(40);
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        final Random random = new Random();
        final CountDownLatch latch = new CountDownLatch(2);
        final Runnable firstAddRunnable = () -> {
            for (int i = 0; i < 20; i++) {
                final String toAdd = "item from first: " + i;
                addSet.add(toAdd);
                suspendQueue.enqueue(toAdd);
                try {
                    Thread.sleep(random.nextInt(100));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            latch.countDown();
        };

        final Runnable secondAddRunnable = () -> {
            for (int i = 0; i < 20; i++) {
                final String toAdd = "item from second:" + i;
                addSet.add(toAdd);
                suspendQueue.enqueue(toAdd);
                try {
                    Thread.sleep(random.nextInt(100));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            latch.countDown();
        };

        executorService.execute(firstAddRunnable);
        executorService.execute(secondAddRunnable);
        latch.await();
        int count = 0;
        String polled = null;
        while ((polled = suspendQueue.dequeue()) != null) {
            count++;
            assertTrue(addSet.contains(polled));
            addSet.remove(polled);
        }
        assertEquals(40, count);
        executorService.shutdown();
    }

}
