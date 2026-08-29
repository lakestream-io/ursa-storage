/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import io.lakestream.api.CatalogPaths;
import io.lakestream.api.LogStateManager;
import io.lakestream.api.LogStorage;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReaderFactory;
import io.lakestream.ursa.lakestream.reader.NoopCompactedObjectReaderFactory;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.OxiaClientFactory;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.UrsaStorage;
import io.lakestream.ursa.storage.impl.EntryIndexCache;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * Self-contained bootstrap service that creates a fully wired {@link IndexedStreamCatalog}.
 *
 * <p>Each {@link #open} call returns an independent catalog that owns its own lifecycle.
 * The caller manages lifecycle via {@code catalog.close()}.
 */
public class StreamCatalogService {

    private static final String COMPACTED_READER_FACTORY_CLASS = "compactedObjectReaderFactoryClass";
    private static final String LEGACY_EXTERNAL_READER_FACTORY_CLASS = "externalReaderFactoryClass";

    @FunctionalInterface
    interface UrsaStorageFactory {
        UrsaStorage create(StorageConfig config, OpenTelemetry otel) throws Exception;
    }

    @FunctionalInterface
    interface CatalogOxiaClientFactory {
        AsyncOxiaClient create(String oxiaUri, StorageConfig config, OpenTelemetry otel) throws Exception;
    }

    private final UrsaStorageFactory ursaStorageFactory;
    private final CatalogOxiaClientFactory oxiaClientFactory;

    public StreamCatalogService() {
        this(UrsaStorage::new);
    }

    StreamCatalogService(UrsaStorageFactory ursaStorageFactory) {
        this(ursaStorageFactory, (oxiaUri, config, otel) ->
            OxiaClientFactory.create(oxiaUri, config.getOxiaStorageConfig(), otel));
    }

    StreamCatalogService(UrsaStorageFactory ursaStorageFactory,
                         CatalogOxiaClientFactory oxiaClientFactory) {
        this.ursaStorageFactory = Objects.requireNonNull(ursaStorageFactory);
        this.oxiaClientFactory = Objects.requireNonNull(oxiaClientFactory);
    }

    /**
     * Opens a {@link IndexedStreamCatalog} with default (noop) OpenTelemetry.
     */
    public IndexedStreamCatalog open(String oxiaUri, Properties properties) throws Exception {
        return open(oxiaUri, new DefaultCatalogPaths(), properties, OpenTelemetry.noop());
    }

    /**
     * Opens a {@link IndexedStreamCatalog} with custom catalog paths and noop OpenTelemetry.
     */
    public IndexedStreamCatalog open(String oxiaUri, CatalogPaths catalogPaths,
                                     Properties properties) throws Exception {
        return open(oxiaUri, catalogPaths, properties, OpenTelemetry.noop());
    }

    /**
     * Opens a {@link IndexedStreamCatalog} with standard catalog paths.
     */
    public IndexedStreamCatalog open(String oxiaUri, Properties properties,
                                     OpenTelemetry otel) throws Exception {
        return open(oxiaUri, new DefaultCatalogPaths(), properties, otel);
    }

    /**
     * Opens a fully wired {@link IndexedStreamCatalog}.
     *
     * <p>Internally creates:
     * <ol>
     *   <li>{@link AsyncOxiaClient} from {@code oxiaUri}</li>
     *   <li>{@link StorageConfig} from properties</li>
     *   <li>{@link UrsaStorage} with the appropriate storage tier</li>
     *   <li>{@link EntryIndexCache}, {@link LogStorage}, {@link LogStateManager} via bridge</li>
     *   <li>The supplied {@link CatalogPaths} strategy</li>
     *   <li>{@link CompactedObjectReaderFactory} from properties</li>
     * </ol>
     *
     * @param oxiaUri      Oxia connection URI (e.g. {@code oxia://host:port/namespace})
     * @param catalogPaths catalog path strategy
     * @param properties   storage configuration properties
     * @param otel         OpenTelemetry instance for observability
     * @return a self-contained catalog; call {@code close()} to release all resources
     */
    public IndexedStreamCatalog open(String oxiaUri, CatalogPaths catalogPaths,
                                     Properties properties, OpenTelemetry otel) throws Exception {
        StorageConfig config = StorageConfig.fromProperties(properties);
        UrsaStorage ursaStorage = createUrsaStorage(config, otel);

        List<AutoCloseable> owned = new ArrayList<>();
        owned.add(ursaStorage);
        return open(oxiaUri, catalogPaths, properties, otel, ursaStorage, owned, null, List.of());
    }

    /**
     * Opens a catalog with a caller-constructed compacted-object reader factory.
     *
     * <p>Ownership of {@code readerFactory} and every entry in {@code additionalOwnedResources}
     * transfers to this method when it is invoked. The factory is initialized here with the same
     * properties and instrumentation as the storage runtime. On success the returned catalog closes
     * all transferred resources; on failure this method closes them before propagating the error.
     *
     * @param oxiaUri Oxia connection URI
     * @param catalogPaths catalog path strategy
     * @param properties storage configuration properties
     * @param otel OpenTelemetry instance used by storage and the reader factory
     * @param readerFactory already constructed reader factory
     * @param additionalOwnedResources resources whose ownership transfers to the returned catalog
     * @return a self-contained catalog
     */
    public IndexedStreamCatalog open(String oxiaUri, CatalogPaths catalogPaths,
                                     Properties properties, OpenTelemetry otel,
                                     CompactedObjectReaderFactory readerFactory,
                                     List<? extends AutoCloseable> additionalOwnedResources) throws Exception {
        Objects.requireNonNull(readerFactory, "readerFactory");
        List<AutoCloseable> additionalOwned = new ArrayList<>();
        UrsaStorage ursaStorage;
        try {
            Objects.requireNonNull(additionalOwnedResources, "additionalOwnedResources");
            for (AutoCloseable resource : additionalOwnedResources) {
                additionalOwned.add(Objects.requireNonNull(
                    resource, "additionalOwnedResources contains null"));
            }
            StorageConfig config = StorageConfig.fromProperties(properties);
            ursaStorage = createUrsaStorage(config, otel);
        } catch (Exception | Error failure) {
            closeReaderFactoryAfterFailure(readerFactory, failure);
            closeOwnedResourcesAfterFailure(additionalOwned, failure);
            throw failure;
        }
        List<AutoCloseable> owned = new ArrayList<>();
        owned.add(ursaStorage);
        return open(oxiaUri, catalogPaths, properties, otel, ursaStorage, owned, readerFactory,
            additionalOwned);
    }

    UrsaStorage createUrsaStorage(StorageConfig config, OpenTelemetry otel) throws Exception {
        return ursaStorageFactory.create(config, otel);
    }

    /**
     * Opens a catalog using an existing {@link UrsaStorage} instance.
     *
     * <p>Use this when multiple catalogs should share a single {@link UrsaStorage}
     * to avoid duplicating object-storage and Oxia client connections.
     *
     * @param oxiaUri      Oxia connection URI
     * @param catalogPaths catalog path strategy
     * @param properties   storage configuration properties
     * @param otel         OpenTelemetry instance
     * @param ursaStorage  shared storage instance (caller manages its lifecycle)
     * @return a catalog that does NOT own the {@link UrsaStorage} lifecycle
     */
    public IndexedStreamCatalog open(String oxiaUri, CatalogPaths catalogPaths,
                                     Properties properties, OpenTelemetry otel,
                                     UrsaStorage ursaStorage) throws Exception {
        return open(oxiaUri, catalogPaths, properties, otel, ursaStorage, new ArrayList<>(), null,
            List.of());
    }

    private IndexedStreamCatalog open(String oxiaUri, CatalogPaths catalogPaths,
                                      Properties properties, OpenTelemetry otel,
                                      UrsaStorage ursaStorage,
                                      List<AutoCloseable> owned,
                                      CompactedObjectReaderFactory suppliedReaderFactory,
                                      List<? extends AutoCloseable> additionalOwnedResources) throws Exception {
        CompactedObjectReaderFactory readerFactory = suppliedReaderFactory;
        boolean additionalResourcesTransferred = false;
        try {
            Objects.requireNonNull(catalogPaths, "catalogPaths");
            StorageConfig config = StorageConfig.fromProperties(properties);
            InstrumentProvider instrumentProvider = new InstrumentProvider(otel);

            // 1. Use the storage-native object WAL implementation.
            StorageApi storageApi = ursaStorage.getDefaultStorageApi();

            // 2. Start WAL cleanup for persistent backends
            if (!"local".equalsIgnoreCase(config.getBackendStorageType())) {
                storageApi.startWALCleanupService();
            }

            // 3. Create Lakestream API bridge types
            LogStorage logStorage = LakestreamBootstrap.createLogStorage(storageApi);
            LogStateManager stateManager = LakestreamBootstrap.createStateManager(storageApi);
            EntryIndexCache cache = LakestreamBootstrap.createEntryIndexCache(storageApi,
                config.getMaxEntryIndexCacheSize(), config.getEntryIndexCacheTTLInSecs());

            // 4. Create AsyncOxiaClient for catalog metadata. Register it immediately so a later
            // bootstrap failure cannot leak the client.
            AsyncOxiaClient oxiaClient = oxiaClientFactory.create(oxiaUri, config, otel);
            owned.add(oxiaClient::close);

            // 5. Create CompactedObjectReaderFactory. Only an absent setting selects the noop
            // implementation; an explicitly configured provider must initialize successfully.
            if (suppliedReaderFactory == null) {
                readerFactory = createCompactedObjectReaderFactory(properties, instrumentProvider);
            } else {
                readerFactory = suppliedReaderFactory;
                readerFactory.initialize(properties, instrumentProvider);
            }

            // 6. Build log factory
            IndexedStreamCatalog.LogFactory logFactory = createLogFactory(
                storageApi, logStorage, cache, stateManager);

            IndexedStreamCatalog catalog = new IndexedStreamCatalog(
                oxiaClient, catalogPaths, logStorage, logFactory,
                stateManager, storageApi::generateStreamId,
                storageApi::getStreamIdByKey,
                storageApi::deleteStreamIdMapping, readerFactory, cache, owned);
            owned.addAll(additionalOwnedResources);
            additionalResourcesTransferred = true;
            return catalog;
        } catch (Exception | Error failure) {
            closeReaderFactoryAfterFailure(readerFactory, failure);
            closeOwnedResourcesAfterFailure(owned, failure);
            if (!additionalResourcesTransferred) {
                closeOwnedResourcesAfterFailure(additionalOwnedResources, failure);
            }
            throw failure;
        }
    }

    static CompactedObjectReaderFactory createCompactedObjectReaderFactory(
            Properties properties, InstrumentProvider instrumentProvider) throws Exception {
        String configuredClass = configuredReaderFactoryClass(properties);
        CompactedObjectReaderFactory readerFactory = configuredClass == null
            ? new NoopCompactedObjectReaderFactory()
            : CompactedObjectReaderFactory.create(configuredClass);
        try {
            readerFactory.initialize(properties, instrumentProvider);
            return readerFactory;
        } catch (Exception | Error failure) {
            closeReaderFactoryAfterFailure(readerFactory, failure);
            throw failure;
        }
    }

    private static String configuredReaderFactoryClass(Properties properties) {
        if (properties == null) {
            return null;
        }
        if (properties.containsKey(COMPACTED_READER_FACTORY_CLASS)) {
            return String.valueOf(properties.get(COMPACTED_READER_FACTORY_CLASS));
        }
        if (properties.containsKey(LEGACY_EXTERNAL_READER_FACTORY_CLASS)) {
            return String.valueOf(properties.get(LEGACY_EXTERNAL_READER_FACTORY_CLASS));
        }
        return null;
    }

    private static void closeReaderFactoryAfterFailure(
            CompactedObjectReaderFactory readerFactory, Throwable failure) {
        if (readerFactory == null) {
            return;
        }
        try {
            readerFactory.close();
        } catch (RuntimeException | Error closeFailure) {
            if (closeFailure != failure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }

    private static void closeOwnedResourcesAfterFailure(
            List<? extends AutoCloseable> ownedResources, Throwable failure) {
        for (int i = ownedResources.size() - 1; i >= 0; i--) {
            try {
                ownedResources.get(i).close();
            } catch (Exception | Error closeFailure) {
                if (closeFailure != failure) {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
    }

    static IndexedStreamCatalog.LogFactory createLogFactory(
            StorageApi storageApi,
            LogStorage logStorage,
            EntryIndexCache cache,
            LogStateManager stateManager) {
        return (name, logId, compactedReader) -> {
            UnifiedStreamReader unifiedReader = compactedReader == null
                ? null
                : new DefaultUnifiedStreamReader(storageApi, compactedReader, cache);
            return new LogImpl(logId, logStorage, unifiedReader, cache, stateManager, unifiedReader != null);
        };
    }
}
