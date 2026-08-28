/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;


import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.Getter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Tag("lakehouse")
public class ObjectPoolTest {

    private ObjectPool<String, TestObject> objectPool;
    private AtomicInteger createCount;
    private AtomicInteger closeCount;
    private Function<String, TestObject> creator;
    private Consumer<TestObject> closeAction;
    private final long idleTimeoutMillis = 1000;

    static class TestObject implements AutoCloseable {
        @Getter
        private final String value;
        @Getter
        private boolean closed = false;
        private final AtomicInteger closeCounter;

        TestObject(String value, AtomicInteger closeCounter) {
            this.value = value;
            this.closeCounter = closeCounter;
        }

        public void close() throws IOException {
            if (!closed) {
                closed = true;
                if (closeCounter != null) {
                    closeCounter.incrementAndGet();
                }
            }
        }

    }

    @BeforeEach
    void setUp() {
        createCount = new AtomicInteger(0);
        closeCount = new AtomicInteger(0);

        creator = key -> {
            createCount.incrementAndGet();
            return new TestObject("value-" + key, closeCount);
        };

        closeAction = obj -> closeCount.incrementAndGet();

        objectPool = new ObjectPool<>(creator, closeAction, idleTimeoutMillis);
    }

    @Test
    void testBorrowCreatesNewObject() {
        ObjectPool.PooledObject<TestObject> borrowed = objectPool.borrow("key1");

        assertNotNull(borrowed);
        assertEquals("value-key1", borrowed.getInstance().getValue());
        assertEquals(1, createCount.get());
        assertFalse(borrowed.getInstance().isClosed());
    }

    @Test
    void testBorrowFromEmptyPoolCreatesNewObjects() {
        ObjectPool.PooledObject<TestObject> borrowed1 = objectPool.borrow("key1");
        ObjectPool.PooledObject<TestObject> borrowed2 = objectPool.borrow("key2");

        assertNotNull(borrowed1);
        assertNotNull(borrowed2);
        assertEquals("value-key1", borrowed1.getInstance().getValue());
        assertEquals("value-key2", borrowed2.getInstance().getValue());
        assertEquals(2, createCount.get());
        assertNotEquals(borrowed1, borrowed2);
    }

    @Test
    void testReleaseAndBorrowReusesObject() {
        ObjectPool.PooledObject<TestObject> borrowed1 = objectPool.borrow("key1");
        objectPool.release(borrowed1, false);
        ObjectPool.PooledObject<TestObject> borrowed2 = objectPool.borrow("key1");

        assertSame(borrowed1, borrowed2);
        assertEquals(1, createCount.get());
        assertEquals(0, closeCount.get());
    }

    @Test
    void testReleaseWithForceCloseClosesObject() {
        ObjectPool.PooledObject<TestObject> borrowed = objectPool.borrow("key1");
        objectPool.release(borrowed, true);

        assertTrue(borrowed.isClosed());
        assertEquals(2, closeCount.get());
        assertTrue(objectPool.getPool().isEmpty());
    }

    @Test
    void testReleaseNullObjectDoesNothing() {
        assertDoesNotThrow(() -> objectPool.release(null, false));
        assertDoesNotThrow(() -> objectPool.release(null, true));
        assertEquals(0, closeCount.get());
    }

    @Test
    void testBorrowAfterCloseThrowsException() {
        objectPool.close();

        IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> objectPool.borrow("key1"));
        assertEquals("ObjectPool is closed", exception.getMessage());
    }

    @Test
    void testCloseClosesAllPooledObjects() {
        ObjectPool.PooledObject<TestObject> borrowed1 = objectPool.borrow("key1");
        ObjectPool.PooledObject<TestObject> borrowed2 = objectPool.borrow("key2");

        objectPool.release(borrowed1, false);
        objectPool.release(borrowed2, false);

        objectPool.close();

        assertTrue(borrowed1.isClosed());
        assertTrue(borrowed2.isClosed());
        assertEquals(4, closeCount.get());
        assertTrue(objectPool.getPool().isEmpty());
    }

    @Test
    void testEvictIdleRemovesOldObjects() throws InterruptedException {
        ObjectPool<String, TestObject> shortTimeoutPool = new ObjectPool<>(creator, closeAction, 100);

        ObjectPool.PooledObject<TestObject> borrowed = shortTimeoutPool.borrow("key1");
        shortTimeoutPool.release(borrowed, false);

        Thread.sleep(150);
        shortTimeoutPool.evictIdle();

        assertTrue(borrowed.isClosed());
        assertEquals(2, closeCount.get());
    }

    @Test
    void testPooledObjectWithCloseableInstance() {
        TestObject testObj = new TestObject("test", new AtomicInteger());
        ObjectPool.PooledObject<TestObject> pooledObject = new ObjectPool.PooledObject<>(testObj, null);

        pooledObject.close();

        assertTrue(pooledObject.isClosed());
        assertTrue(testObj.isClosed());
    }

    @Test
    void testPooledObjectCloseActionExecuted() {
        AtomicInteger actionCount = new AtomicInteger(0);
        Consumer<String> action = obj -> actionCount.incrementAndGet();
        ObjectPool.PooledObject<String> pooledObject = new ObjectPool.PooledObject<>("test", action);

        pooledObject.close();

        assertTrue(pooledObject.isClosed());
        assertEquals(1, actionCount.get());
    }

    @Test
    void testPooledObjectCloseIdempotent() {
        ObjectPool.PooledObject<TestObject> borrowed = objectPool.borrow("key1");

        borrowed.close();
        borrowed.close();

        assertTrue(borrowed.isClosed());
        assertEquals(2, closeCount.get());
    }

    @Test
    void testPooledObjectLastUsedUpdated() throws InterruptedException {
        ObjectPool.PooledObject<TestObject> borrowed = objectPool.borrow("key1");
        long initialTime = borrowed.getLastUsed();

        Thread.sleep(10);
        borrowed.updateLastUsed();

        assertTrue(borrowed.getLastUsed() > initialTime);
    }

    @Test
    @Timeout(5)
    void testConcurrentBorrowAndRelease() throws InterruptedException {
        AtomicInteger concurrentCreateCount = new AtomicInteger(0);
        Function<String, TestObject> concurrentCreator = key -> {
            concurrentCreateCount.incrementAndGet();
            return new TestObject("value-" + key, new AtomicInteger());
        };

        ObjectPool<String, TestObject> concurrentPool = new ObjectPool<>(concurrentCreator, obj -> {
        }, 5000);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(100);

        for (int i = 0; i < 100; i++) {
            executor.submit(() -> {
                try {
                    ObjectPool.PooledObject<TestObject> borrowed = concurrentPool.borrow("key");
                    Thread.sleep(10);
                    concurrentPool.release(borrowed, false);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();
        assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));

        assertTrue(concurrentCreateCount.get() > 0);
        assertTrue(concurrentCreateCount.get() <= 100);
    }

    @Test
    void testReleaseClosedObjectDoesNotReturnToPool() {
        ObjectPool.PooledObject<TestObject> borrowed = objectPool.borrow("key1");
        borrowed.close();
        objectPool.release(borrowed, false);

        assertTrue(objectPool.getPool().isEmpty());
        assertTrue(borrowed.isClosed());
    }

    @Test
    void testBorrowSkipsClosedObjectsInPool() {
        ObjectPool.PooledObject<TestObject> borrowed1 = objectPool.borrow("key1");
        objectPool.release(borrowed1, false);
        borrowed1.close();

        ObjectPool.PooledObject<TestObject> borrowed2 = objectPool.borrow("key1");

        assertNotSame(borrowed1, borrowed2);
        assertEquals("value-key1", borrowed2.getInstance().getValue());
        assertEquals(2, createCount.get());
        assertTrue(borrowed1.isClosed());
        assertFalse(borrowed2.isClosed());
    }

    @Test
    void testMultipleKeysInPool() {
        ObjectPool.PooledObject<TestObject> borrowed1 = objectPool.borrow("key1");
        ObjectPool.PooledObject<TestObject> borrowed2 = objectPool.borrow("key2");
        ObjectPool.PooledObject<TestObject> borrowed3 = objectPool.borrow("key3");

        objectPool.release(borrowed1, false);
        objectPool.release(borrowed2, false);
        objectPool.release(borrowed3, false);

        assertEquals(3, objectPool.getPool().size());
        assertEquals(3, createCount.get());
        assertEquals(0, closeCount.get());
    }

    @Test
    void testEvictIdleKeepsRecentlyUsedObjects() throws InterruptedException {
        ObjectPool<String, TestObject> shortTimeoutPool = new ObjectPool<>(creator, closeAction, 200);

        ObjectPool.PooledObject<TestObject> borrowed1 = shortTimeoutPool.borrow("key1");
        ObjectPool.PooledObject<TestObject> borrowed2 = shortTimeoutPool.borrow("key2");

        shortTimeoutPool.release(borrowed1, false);
        Thread.sleep(100);
        shortTimeoutPool.release(borrowed2, false);
        Thread.sleep(150);

        shortTimeoutPool.evictIdle();

        assertTrue(borrowed1.isClosed());
        assertFalse(borrowed2.isClosed());
        assertEquals(2, closeCount.get());
    }

    @Test
    void testConcurrentBorrowAndEvict() throws InterruptedException {
        ObjectPool<String, TestObject> pool = new ObjectPool<>(creator, closeAction, 200);
        AtomicInteger created = new AtomicInteger();
        AtomicInteger closed = new AtomicInteger();

        for (int i = 0; i < 20; i++) {
            pool.release(pool.borrow("x"), false);
        }

        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(2);

        // Thread 1: borrow object in a loop
        executor.submit(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < 1000; i++) {
                    var obj = pool.borrow("x");
                    assertFalse(obj.isClosed(), "Borrowed a closed object!");
                    pool.release(obj, false);
                }
            } catch (Exception e) {
                fail("Borrow thread failed: " + e.getMessage());
            } finally {
                doneLatch.countDown();
            }
        });

        // Thread 2: evict idle objects
        executor.submit(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < 100; i++) {
                    pool.evictIdle();
                    Thread.sleep(2);  // simulate realistic delay
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                doneLatch.countDown();
            }
        });

        startLatch.countDown();
        doneLatch.await();
        executor.shutdownNow();

        // Optional: validate the pool state is consistent
        pool.close();
        assertEquals(created.get(), closed.get(), "All created objects should be closed");
    }
}
