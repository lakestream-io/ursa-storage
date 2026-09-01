/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.Log;
import io.lakestream.api.LogEntry;
import io.lakestream.api.LogId;
import io.lakestream.api.LogStateManager;
import io.lakestream.api.LogStorage;
import io.lakestream.api.Position;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReader;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReaderFactory;
import io.lakestream.ursa.lakestream.reader.NoopCompactedObjectReaderFactory;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.UrsaStorage;
import io.lakestream.ursa.storage.impl.EntryIndexCache;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.netty.buffer.Unpooled;
import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class StreamCatalogServiceTest {

    private static final String READER_FACTORY_PROPERTY = "compactedObjectReaderFactoryClass";

    @Test
    void bootstrapCreatesProtocolNeutralStorage() throws Exception {
        StorageConfig config = mock(StorageConfig.class);
        OpenTelemetry otel = OpenTelemetry.noop();
        UrsaStorage storage = mock(UrsaStorage.class);
        StreamCatalogService.UrsaStorageFactory factory = mock(StreamCatalogService.UrsaStorageFactory.class);
        when(factory.create(config, otel)).thenReturn(storage);

        StreamCatalogService service = new StreamCatalogService(factory);

        assertSame(storage, service.createUrsaStorage(config, otel));
        verify(factory).create(config, otel);
    }

    @Test
    void configuredReaderReachesCompactedRoutingAndIsOwnedByLog() throws Exception {
        StorageApi storageApi = mock(StorageApi.class);
        LogStorage logStorage = mock(LogStorage.class);
        EntryIndexCache cache = mock(EntryIndexCache.class);
        LogStateManager stateManager = mock(LogStateManager.class);
        CompactedObjectReader compactedReader = mock(CompactedObjectReader.class);
        LogId logId = LogId.of(201L);
        long startOffset = 50L;
        EntryHeader header = new EntryHeader(startOffset, 3, 2_000L, 10, 10);
        EntryIndex entryIndex = new EntryIndex(
            header,
            new Position("parquet-file", 0, Position.FileType.PARQUET),
            1,
            EntryIndex.IndexType.COMPACT,
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
        try (LogEntry compactedEntry =
                Entry.of(header, Unpooled.wrappedBuffer(new byte[] {1, 2, 3})).toLogEntry()) {
            when(cache.get(logId.id(), startOffset))
                .thenReturn(CompletableFuture.completedFuture(entryIndex));
            when(compactedReader.readMessagesWithEntryIndexAsync(
                eq(entryIndex), eq(startOffset), eq(startOffset), eq(10L), eq(1024L)))
                .thenReturn(CompletableFuture.completedFuture(
                    new CompactedObjectReader.ReadResult(false, List.of(compactedEntry))));

            IndexedStreamCatalog.LogFactory logFactory = StreamCatalogService.createLogFactory(
                storageApi, logStorage, cache, stateManager);
            Log log = logFactory.create("default/stream-partition-0", logId, compactedReader);

            assertEquals(List.of(compactedEntry), log.readEntries(startOffset, 10, 1024L).get());
            verify(compactedReader).readMessagesWithEntryIndexAsync(
                entryIndex, startOffset, startOffset, 10L, 1024L);
            verifyNoInteractions(logStorage);

            log.close();
            log.close();
            verify(compactedReader).close();
        }
    }

    @Test
    void absentReaderFactoryConfigurationUsesNoop() throws Exception {
        CompactedObjectReaderFactory readerFactory =
            StreamCatalogService.createCompactedObjectReaderFactory(
                new Properties(), InstrumentProvider.NOOP);

        assertInstanceOf(NoopCompactedObjectReaderFactory.class, readerFactory);

        readerFactory.close();
    }

    @Test
    void explicitlyConfiguredReaderConstructionFailureFailsStartupAndClosesResources() throws Exception {
        Properties properties = localProperties();
        properties.setProperty(READER_FACTORY_PROPERTY, ConstructorFailingReaderFactory.class.getName());
        UrsaStorage storage = mock(UrsaStorage.class);
        StorageApi storageApi = mock(StorageApi.class);
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        StreamCatalogService service = bootstrapService(storage, storageApi, oxiaClient);

        Exception failure = assertThrows(Exception.class,
            () -> service.open("oxia://unused/catalog", properties));

        assertInstanceOf(ReflectiveOperationException.class, failure.getCause());
        InOrder closeOrder = inOrder(oxiaClient, storage);
        closeOrder.verify(oxiaClient).close();
        closeOrder.verify(storage).close();
    }

    @Test
    void explicitlyConfiguredReaderInitializationFailureClosesFactoryAndAllResources() throws Exception {
        InitializationFailingReaderFactory.reset();
        Properties properties = localProperties();
        properties.setProperty(READER_FACTORY_PROPERTY, InitializationFailingReaderFactory.class.getName());
        UrsaStorage storage = mock(UrsaStorage.class);
        StorageApi storageApi = mock(StorageApi.class);
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        RuntimeException oxiaCloseFailure = new RuntimeException("oxia close failed");
        doThrow(oxiaCloseFailure).when(oxiaClient).close();
        StreamCatalogService service = bootstrapService(storage, storageApi, oxiaClient);

        IllegalStateException failure = assertThrows(IllegalStateException.class,
            () -> service.open("oxia://unused/catalog", properties));

        assertEquals("configured reader init failed", failure.getMessage());
        assertEquals(1, InitializationFailingReaderFactory.INITIALIZE_COUNT.get());
        assertEquals(1, InitializationFailingReaderFactory.CLOSE_COUNT.get());
        assertEquals(1, failure.getSuppressed().length);
        assertSame(oxiaCloseFailure, failure.getSuppressed()[0]);
        verify(oxiaClient).close();
        verify(storage).close();
    }

    @Test
    void injectedReaderAndAdditionalResourcesAreClosedWhenBootstrapFailsBeforeInitialization()
            throws Exception {
        Properties properties = localProperties();
        UrsaStorage storage = mock(UrsaStorage.class);
        CompactedObjectReaderFactory readerFactory = mock(CompactedObjectReaderFactory.class);
        AutoCloseable additionalResource = mock(AutoCloseable.class);
        IllegalStateException bootstrapFailure = new IllegalStateException("storage api unavailable");
        when(storage.getDefaultStorageApi()).thenThrow(bootstrapFailure);
        StreamCatalogService service = new StreamCatalogService(
            (config, otel) -> storage,
            (oxiaUri, config, otel) -> mock(AsyncOxiaClient.class));

        IllegalStateException failure = assertThrows(IllegalStateException.class,
            () -> service.open("oxia://unused/catalog", new DefaultCatalogPaths(), properties,
                OpenTelemetry.noop(), readerFactory, List.of(additionalResource)));

        assertSame(bootstrapFailure, failure);
        verify(readerFactory, never()).initialize(any(), any());
        verify(readerFactory).close();
        verify(storage).close();
        verify(additionalResource).close();
    }

    @Test
    void injectedResourcesTransferToCatalogInDependencyOrder() throws Exception {
        Properties properties = localProperties();
        UrsaStorage storage = mock(UrsaStorage.class);
        StorageApi storageApi = mock(StorageApi.class);
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        CompactedObjectReaderFactory readerFactory = mock(CompactedObjectReaderFactory.class);
        AutoCloseable telemetry = mock(AutoCloseable.class);
        when(storage.getDefaultStorageApi()).thenReturn(storageApi);
        when(storageApi.getStreamStateManager()).thenReturn(mock(LogStateManager.class));
        StreamCatalogService service = bootstrapService(storage, storageApi, oxiaClient);

        IndexedStreamCatalog catalog = service.open(
            "oxia://unused/catalog", new DefaultCatalogPaths(), properties, OpenTelemetry.noop(),
            readerFactory, List.of(telemetry));
        catalog.close();

        InOrder closeOrder = inOrder(readerFactory, storage, oxiaClient, telemetry);
        closeOrder.verify(readerFactory).close();
        closeOrder.verify(storage).close();
        closeOrder.verify(oxiaClient).close();
        closeOrder.verify(telemetry).close();
    }

    @Test
    void injectedReaderInitializationFailureClosesTelemetryAfterStorageDependencies()
            throws Exception {
        Properties properties = localProperties();
        UrsaStorage storage = mock(UrsaStorage.class);
        StorageApi storageApi = mock(StorageApi.class);
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        CompactedObjectReaderFactory readerFactory = mock(CompactedObjectReaderFactory.class);
        AutoCloseable telemetry = mock(AutoCloseable.class);
        when(storage.getDefaultStorageApi()).thenReturn(storageApi);
        when(storageApi.getStreamStateManager()).thenReturn(mock(LogStateManager.class));
        IllegalStateException initializationFailure = new IllegalStateException("reader init failed");
        doThrow(initializationFailure).when(readerFactory).initialize(any(), any());
        StreamCatalogService service = bootstrapService(storage, storageApi, oxiaClient);

        assertSame(initializationFailure, assertThrows(IllegalStateException.class,
            () -> service.open("oxia://unused/catalog", new DefaultCatalogPaths(), properties,
                OpenTelemetry.noop(), readerFactory, List.of(telemetry))));

        InOrder closeOrder = inOrder(readerFactory, oxiaClient, storage, telemetry);
        closeOrder.verify(readerFactory).close();
        closeOrder.verify(oxiaClient).close();
        closeOrder.verify(storage).close();
        closeOrder.verify(telemetry).close();
    }

    private static StreamCatalogService bootstrapService(
            UrsaStorage storage, StorageApi storageApi, AsyncOxiaClient oxiaClient) {
        when(storage.getDefaultStorageApi()).thenReturn(storageApi);
        when(storageApi.getStreamStateManager()).thenReturn(mock(LogStateManager.class));
        return new StreamCatalogService(
            (config, otel) -> storage,
            (oxiaUri, config, otel) -> oxiaClient);
    }

    private static Properties localProperties() {
        Properties properties = new Properties();
        properties.setProperty("backendStorageType", "local");
        return properties;
    }

    public static final class ConstructorFailingReaderFactory implements CompactedObjectReaderFactory {

        public ConstructorFailingReaderFactory() {
            throw new IllegalStateException("configured reader construction failed");
        }

        @Override
        public void initialize(Properties properties, InstrumentProvider provider) {
        }

        @Override
        public CompactedObjectReader open(String name) {
            throw new AssertionError("unreachable");
        }

        @Override
        public void close() {
        }
    }

    public static final class InitializationFailingReaderFactory implements CompactedObjectReaderFactory {

        private static final AtomicInteger INITIALIZE_COUNT = new AtomicInteger();
        private static final AtomicInteger CLOSE_COUNT = new AtomicInteger();

        static void reset() {
            INITIALIZE_COUNT.set(0);
            CLOSE_COUNT.set(0);
        }

        @Override
        public void initialize(Properties properties, InstrumentProvider provider) {
            INITIALIZE_COUNT.incrementAndGet();
            throw new IllegalStateException("configured reader init failed");
        }

        @Override
        public CompactedObjectReader open(String name) {
            throw new AssertionError("unreachable");
        }

        @Override
        public void close() {
            CLOSE_COUNT.incrementAndGet();
        }
    }
}
