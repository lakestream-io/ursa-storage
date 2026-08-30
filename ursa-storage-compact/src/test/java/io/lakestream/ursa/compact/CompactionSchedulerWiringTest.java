/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.StreamIdentifier;
import io.lakestream.ursa.materialization.MaterializationRuntime;
import io.lakestream.ursa.materialization.MaterializationService;
import io.lakestream.ursa.materialization.MaterializationServiceConfig;
import io.lakestream.ursa.materialization.MaterializationServiceProvider;
import io.lakestream.ursa.materialization.MaterializationTask;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.compaction.CompactionStorageBindings;
import io.lakestream.ursa.storage.impl.compaction.StartStopRunner;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.InOrder;

/**
 * Smoke test for the {@link CompactionScheduler} reflective wiring.
 *
 * <p>Covers two paths:
 * <ul>
 *   <li>{@code materializationServiceClass} is honoured when set (new config key).</li>
 *   <li>The deprecated {@code compactionServiceClass} alias is honoured with a fallback when
 *       the new key is absent.</li>
 * </ul>
 *
 * <p>The test exercises the static class-name resolution helper on
 * {@link CompactionScheduler} via reflection, since standing up a full scheduler requires Oxia
 * + downstream wiring that is out of scope for unit tests.
 */
public class CompactionSchedulerWiringTest {

    /**
     * A {@link MaterializationService} stub instantiable by reflection; used to verify the
     * {@code materializationServiceClass} key drives the right class through the loader.
     */
    public static class FakeMaterializationService implements MaterializationService {
        public FakeMaterializationService() {
        }

        @Override
        public void initialize(MaterializationRuntime runtime, MaterializationServiceConfig config) {
        }

        @Override
        public void materialize(MaterializationTask task) {
        }

        @Override
        public void invalidate(StreamIdentifier id) {
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void materializationServiceClassLoadsConfiguredClass() {
        MaterializationService svc = MaterializationServiceProvider.load(
                FakeMaterializationService.class.getName());
        assertNotNull(svc);
        assertSame(FakeMaterializationService.class, svc.getClass());
    }

    @Test
    public void newKeyHonouredWhenSet() throws Exception {
        StorageConfig config = StorageConfig.builder().build();
        Properties props = new Properties();
        props.setProperty("materializationServiceClass", FakeMaterializationService.class.getName());
        config.setProperties(props);
        config.setMaterializationServiceClass(FakeMaterializationService.class.getName());

        assertEquals(FakeMaterializationService.class.getName(),
                invokeResolveMaterializationServiceClass(config));
    }

    @Test
    public void legacyAliasFallsBackWithWarn() throws Exception {
        StorageConfig config = StorageConfig.builder().build();
        Properties props = new Properties();
        props.setProperty("compactionServiceClass", FakeMaterializationService.class.getName());
        config.setProperties(props);
        config.setCompactionServiceClass(FakeMaterializationService.class.getName());

        assertEquals(FakeMaterializationService.class.getName(),
                invokeResolveMaterializationServiceClass(config));
    }

    @Test
    public void legacyDefaultMapsToNewDefault() throws Exception {
        StorageConfig config = StorageConfig.builder().build();
        Properties props = new Properties();
        // Caller still sets the legacy key with the historical default; the resolver should
        // map it to the new default (LakehouseMaterializationService) rather than the
        // pre-T10 LakehouseCompactionServiceImpl.
        props.setProperty("compactionServiceClass",
                "io.lakestream.ursa.lakehouse.compact.LakehouseCompactionServiceImpl");
        config.setProperties(props);
        config.setCompactionServiceClass(
                "io.lakestream.ursa.lakehouse.compact.LakehouseCompactionServiceImpl");

        assertEquals(config.getMaterializationServiceClass(),
                invokeResolveMaterializationServiceClass(config));
    }

    @Test
    public void disabledInternalTaskPublisherDoesNotStartPublisher() throws Exception {
        CompactionScheduler scheduler = mock(CompactionScheduler.class, Answers.CALLS_REAL_METHODS);
        StorageConfig config = StorageConfig.builder()
                .internalCompactionTaskPublisherEnabled(false)
                .build();
        CompactionStorageBindings storageBindings = mock(CompactionStorageBindings.class);
        setField(scheduler, "config", config);
        setField(scheduler, "storageBindings", storageBindings);

        invokeStartPublishCompactTaskRunner(scheduler);

        verify(storageBindings, never()).createPublishCompactTaskRunner();
    }

    @Test
    public void legacyExternalPublisherModeDoesNotRequireStreamCatalog() {
        StorageConfig config = StorageConfig.builder()
                .internalCompactionTaskPublisherEnabled(false)
                .materializationEnabled(false)
                .build();

        assertFalse(CompactionScheduler.requiresStreamCatalog(config));

        config.setInternalCompactionTaskPublisherEnabled(true);
        assertTrue(CompactionScheduler.requiresStreamCatalog(config));
        config.setInternalCompactionTaskPublisherEnabled(false);
        config.setMaterializationEnabled(true);
        assertTrue(CompactionScheduler.requiresStreamCatalog(config));
    }

    @Test
    public void enabledInternalTaskPublisherStartsCatalogPublisherWithoutLegacyTopicRefresh() throws Exception {
        CompactionScheduler scheduler = mock(CompactionScheduler.class, Answers.CALLS_REAL_METHODS);
        StorageConfig config = StorageConfig.builder()
                .refreshLocalTopicInternalInSeconds(7)
                .build();
        CompactionStorageBindings storageBindings = mock(CompactionStorageBindings.class);
        StartStopRunner runner = mock(StartStopRunner.class);
        when(storageBindings.createPublishCompactTaskRunner()).thenReturn(runner);
        setField(scheduler, "config", config);
        setField(scheduler, "storageBindings", storageBindings);

        invokeStartPublishCompactTaskRunner(scheduler);

        verify(storageBindings).createPublishCompactTaskRunner();
        verify(runner).start();
    }

    @Test
    public void shutdownNowRunsOnlyWhenGracefulShutdownTimesOut() throws Exception {
        ExecutorService timedOut = mock(ExecutorService.class);
        when(timedOut.awaitTermination(1, TimeUnit.MILLISECONDS)).thenReturn(false);
        CompactionScheduler.shutdownExecutor(timedOut, 1, TimeUnit.MILLISECONDS);
        verify(timedOut).shutdown();
        verify(timedOut).shutdownNow();
        verify(timedOut, times(2)).awaitTermination(1, TimeUnit.MILLISECONDS);

        ExecutorService terminated = mock(ExecutorService.class);
        when(terminated.awaitTermination(1, TimeUnit.MILLISECONDS)).thenReturn(true);
        CompactionScheduler.shutdownExecutor(terminated, 1, TimeUnit.MILLISECONDS);
        verify(terminated).shutdown();
        verify(terminated, never()).shutdownNow();
    }

    @Test
    public void interruptedShutdownForcesAndAwaitsTerminationBeforeRestoringInterrupt() throws Exception {
        ExecutorService interrupted = mock(ExecutorService.class);
        when(interrupted.awaitTermination(1, TimeUnit.MILLISECONDS))
                .thenThrow(new InterruptedException("shutdown interrupted"))
                .thenReturn(true);

        try {
            assertThrows(InterruptedException.class,
                    () -> CompactionScheduler.shutdownExecutor(interrupted, 1, TimeUnit.MILLISECONDS));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
        verify(interrupted).shutdownNow();
        verify(interrupted, times(2)).awaitTermination(1, TimeUnit.MILLISECONDS);
    }

    @Test
    public void forcedExecutorTerminationPrecedesDependentResourceClose() throws Exception {
        CompactionScheduler scheduler = mock(CompactionScheduler.class, Answers.CALLS_REAL_METHODS);
        ExecutorService workerExecutor = mock(ExecutorService.class);
        MaterializationService materialization = mock(MaterializationService.class);
        setField(scheduler, "executor", workerExecutor);
        setField(scheduler, "materializationService", materialization);
        CountDownLatch forcedAwaitStarted = new CountDownLatch(1);
        CountDownLatch allowWorkerTermination = new CountDownLatch(1);
        when(workerExecutor.awaitTermination(10, TimeUnit.SECONDS))
                .thenReturn(false)
                .thenAnswer(invocation -> {
                    forcedAwaitStarted.countDown();
                    return allowWorkerTermination.await(10, TimeUnit.SECONDS);
                });

        CompletableFuture<Void> closing = CompletableFuture.runAsync(() -> {
            try {
                scheduler.close();
            } catch (InterruptedException error) {
                throw new AssertionError(error);
            }
        });
        assertTrue(forcedAwaitStarted.await(10, TimeUnit.SECONDS));
        assertFalse(closing.isDone());
        verify(materialization, never()).close();

        allowWorkerTermination.countDown();
        closing.get(10, TimeUnit.SECONDS);

        InOrder order = inOrder(workerExecutor, materialization);
        order.verify(workerExecutor).shutdown();
        order.verify(workerExecutor).awaitTermination(10, TimeUnit.SECONDS);
        order.verify(workerExecutor).shutdownNow();
        order.verify(workerExecutor).awaitTermination(10, TimeUnit.SECONDS);
        order.verify(materialization).close();
    }

    private static String invokeResolveMaterializationServiceClass(StorageConfig config) throws Exception {
        Method m = CompactionScheduler.class.getDeclaredMethod(
                "resolveMaterializationServiceClass", StorageConfig.class);
        m.setAccessible(true);
        return (String) m.invoke(null, config);
    }

    private static void invokeStartPublishCompactTaskRunner(CompactionScheduler scheduler) throws Exception {
        Method method = CompactionScheduler.class.getDeclaredMethod("startPublishCompactTaskRunner");
        method.setAccessible(true);
        method.invoke(scheduler);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        var field = CompactionScheduler.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
