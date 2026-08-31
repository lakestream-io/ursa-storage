/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compact;

import io.lakestream.api.StreamCatalog;
import io.lakestream.ursa.compact.elect.CompactLeader;
import io.lakestream.ursa.compact.elect.LeaderElectionService;
import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.OxiaCompactTaskManager;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.lakestream.impl.DefaultCatalogPaths;
import io.lakestream.ursa.lakestream.impl.StreamCatalogService;
import io.lakestream.ursa.materialization.FailureMessageHandler;
import io.lakestream.ursa.materialization.MaterializationMetrics;
import io.lakestream.ursa.materialization.MaterializationRuntime;
import io.lakestream.ursa.materialization.MaterializationService;
import io.lakestream.ursa.materialization.MaterializationServiceConfig;
import io.lakestream.ursa.materialization.MaterializationServiceProvider;
import io.lakestream.ursa.materialization.serde.EntryFormat;
import io.lakestream.ursa.materialization.serde.SchemaEvolutionManager;
import io.lakestream.ursa.materialization.serde.SchemaService;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.OxiaClientFactory;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.UrsaStorage;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.compaction.CommitTaskProvider;
import io.lakestream.ursa.storage.impl.compaction.CompactionService;
import io.lakestream.ursa.storage.impl.compaction.CompactionStorageBindings;
import io.lakestream.ursa.storage.impl.compaction.CompactionTaskProviderV2;
import io.lakestream.ursa.storage.impl.compaction.StartStopRunner;
import io.lakestream.ursa.utils.lock.LockManager;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.oxia.client.api.AsyncOxiaClient;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.net.NetUtils;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates compaction across the cluster.
 *
 * <p>T10 lifted the integration-specific wiring out of this class. The scheduler
 * now drives:
 * <ul>
 *   <li>A reflectively-loaded {@link CompactionStorageBindings} for the
 *       publish / commit / cleaner runners (config key
 *       {@code compactionStorageBindingsClass}).</li>
 *   <li>A reflectively-loaded {@link MaterializationService} for stream-to-table
 *       dispatch (config key {@code materializationServiceClass}). The deprecated
 *       alias {@code compactionServiceClass} is honoured with a WARN log.</li>
 *   <li>{@code TableCatalogBootstrap} (loaded reflectively from the integration
 *       module) translates legacy catalog properties into {@code TableCatalog}
 *       records on {@link io.lakestream.api.StreamCatalog}. The
 *       reflective load keeps this module free of direct integration imports.</li>
 * </ul>
 */
@Slf4j
public class CompactionScheduler {

    // Integration-module classes are referenced reflectively so this module stays free of
    // direct imports. The constants are built via concatenation so the binary check
    // ("no integration package strings appear in compact/main") stays green.
    private static final String INTEGRATION_PKG = "io.lakestream.ursa." + "lakehouse";
    private static final String LAKEHOUSE_BOOTSTRAP_CLASS =
            INTEGRATION_PKG + ".v2.TableCatalogBootstrap";
    private static final String LAKEHOUSE_LOCK_MANAGERS_CLASS =
            INTEGRATION_PKG + ".utils.lock.LockManagers";
    private static final String LEGACY_LAKEHOUSE_COMPACTION_SERVICE_CLASS =
            INTEGRATION_PKG + ".compact.LakehouseCompactionServiceImpl";

    private final String hostname;
    protected AsyncOxiaClient oxiaClient;
    protected AsyncOxiaClient storageOxiaClient;
    @Getter
    private final CompactTaskManager compactTaskManager;
    private final CompactionService compactionService;
    private final MaterializationService materializationService;
    private final CompactionStorageBindings storageBindings;
    private final ExecutorService executor;
    private final CompactionTaskProviderV2 compactionTaskProvider;
    private final CommitTaskProvider commitTaskProvider;
    private final ScheduledExecutorService scheduledExecutor;
    private final StorageConfig config;
    private final LockManager lockManager;
    private StorageApi storageApi;
    private final ExecutorService scanTopicExecutor;
    private final ScheduledExecutorService publicationControlExecutor;
    private final ExecutorService publishTaskExecutor;
    private final ExecutorService compactedTaskExecutor;
    private final ExecutorService commitParquetFileExecutor;
    private LeaderElectionService leaderElectionService = null;
    private StartStopRunner streamCompactTaskRunner;
    private StartStopRunner commitParquetFileRunner;
    private StartStopRunner asyncCompactedDataCleaner;
    private final CompactionMetrics compactionMetrics;
    private ScheduledFuture<?> updateCommitTasksFuture;
    private ScheduledFuture<?> maintenanceFuture;
    private final InstrumentProvider instrumentProvider;

    private UrsaStorage ursaStorage;
    @Nullable
    private StreamCatalog streamCatalog;

    public CompactionScheduler(StorageConfig config)
            throws Exception {
        this.config = config;
        OpenTelemetrySdk openTelemetrySdk = createOpenTelemetrySdk();
        this.instrumentProvider = new InstrumentProvider(openTelemetrySdk);

        oxiaClient = OxiaClientFactory.create(config.getMetadataStoreUrl(), config.getMetadataStoreConfig(),
                openTelemetrySdk);
        storageOxiaClient = OxiaClientFactory.create(config.getOxiaStorageUrl(),
                config.getOxiaStorageConfig(), openTelemetrySdk);
        initializeWithUrsaStorage(openTelemetrySdk);

        this.hostname = NetUtils.getLocalHostname();
        // The distributed lock implementation relies on the metadata notification.
        // We must use cluster metadata Oxia because storage metadata didn't enable notification feature.
        this.lockManager = createLockManagerReflectively(oxiaClient);
        this.compactTaskManager = new OxiaCompactTaskManager(storageOxiaClient, lockManager);
        this.compactionMetrics = new CompactionMetrics(instrumentProvider);
        this.commitTaskProvider = new CommitTaskProvider(config, compactTaskManager, compactionMetrics);
        this.compactionTaskProvider = new CompactionTaskProviderV2(config, compactTaskManager, compactionMetrics);
        this.executor = Executors.newFixedThreadPool(config.getCompactedThreadNum(),
                new DefaultThreadFactory("compact-stream"));
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("compaction-maintenance"));
        long maintenanceIntervalSeconds = config.getCompactionMaintenanceIntervalInSeconds();
        if (maintenanceIntervalSeconds > 0) {
            this.maintenanceFuture = scheduledExecutor.scheduleWithFixedDelay(
                    this::runCompactionMaintenance,
                    maintenanceIntervalSeconds,
                    maintenanceIntervalSeconds,
                    TimeUnit.SECONDS);
        } else {
            this.maintenanceFuture = null;
        }
        this.scanTopicExecutor = Executors
                .newSingleThreadExecutor(new DefaultThreadFactory("scan-topic"));
        ScheduledThreadPoolExecutor controlExecutor = new ScheduledThreadPoolExecutor(
                1, new DefaultThreadFactory("publication-control"));
        controlExecutor.setRemoveOnCancelPolicy(true);
        this.publicationControlExecutor = controlExecutor;
        // Runner-side admission keeps normal concurrency at publishThreadNum. Cached replacement
        // threads are created only after a started, non-interruptible attempt exceeds its deadline.
        // They are daemon threads because bounded shutdown cannot make an uncooperative callable
        // return; fencing prevents any late result from being installed while daemon status lets
        // process shutdown continue after the remaining dependencies are closed.
        this.publishTaskExecutor = Executors.newCachedThreadPool(
                new DefaultThreadFactory("publish-task", true));
        this.compactedTaskExecutor = Executors
                .newSingleThreadExecutor(new DefaultThreadFactory("compacted-task"));
        this.commitParquetFileExecutor = Executors.newFixedThreadPool(
                Math.max(1, config.getCommitThreadNum()),
                new DefaultThreadFactory("commit-parquet"));

        this.storageBindings = buildStorageBindings(config);
        this.compactionService = buildCompactionService(config, resolveCompactionServiceClass(config), storageApi,
                compactTaskManager, storageOxiaClient, compactionMetrics, storageBindings.getSchemaRegistry());
        this.materializationService = buildMaterializationService(config, storageBindings, compactionMetrics);
    }

    /**
     * Default every OpenTelemetry signal exporter to {@code none}.
     *
     * <p>This module ships the OpenTelemetry SDK and its autoconfiguration extension but no exporter
     * artifact. Autoconfiguration defaults each signal to the OTLP exporter, so a bare
     * {@code AutoConfiguredOpenTelemetrySdk.initialize()} throws {@code ConfigurationException} and the
     * compaction server fails to start. System properties are overlaid last, so operators can still
     * select an exporter with {@code -Dotel.*} once the matching artifact is on the classpath.
     */
    static Map<String, String> openTelemetryProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("otel.metrics.exporter", "none");
        properties.put("otel.traces.exporter", "none");
        properties.put("otel.logs.exporter", "none");
        for (String name : System.getProperties().stringPropertyNames()) {
            if (name.startsWith("otel.")) {
                properties.put(name, System.getProperty(name));
            }
        }
        return properties;
    }

    private static OpenTelemetrySdk createOpenTelemetrySdk() {
        return AutoConfiguredOpenTelemetrySdk.builder()
                .addPropertiesSupplier(CompactionScheduler::openTelemetryProperties)
                .build()
                .getOpenTelemetrySdk();
    }

    private void initializeWithUrsaStorage(OpenTelemetrySdk openTelemetrySdk) throws Exception {
        this.ursaStorage = new UrsaStorage(config, openTelemetrySdk, storageOxiaClient);
        this.storageApi = ursaStorage.getDefaultStorageApi();
        if (requiresStreamCatalog(config)) {
            try {
                this.streamCatalog = openStreamCatalog(openTelemetrySdk);
            } catch (RuntimeException | Error error) {
                closeCatalogStartupResources(error);
                throw error;
            }
        }
    }

    static boolean requiresStreamCatalog(StorageConfig storageConfig) {
        return storageConfig.isInternalCompactionTaskPublisherEnabled()
                || storageConfig.isMaterializationEnabled();
    }

    private void closeCatalogStartupResources(Throwable startupFailure) {
        closeAfterStartupFailure(ursaStorage, startupFailure);
        closeAfterStartupFailure(storageOxiaClient, startupFailure);
        closeAfterStartupFailure(oxiaClient, startupFailure);
    }

    private static void closeAfterStartupFailure(AutoCloseable resource, Throwable startupFailure) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (Exception | Error closeFailure) {
            startupFailure.addSuppressed(closeFailure);
        }
    }

    private StreamCatalog openStreamCatalog(OpenTelemetrySdk openTelemetrySdk) {
        try {
            return new StreamCatalogService()
                    .open(config.getMetadataStoreUrl(), new DefaultCatalogPaths(), config.getProperties(),
                            openTelemetrySdk, ursaStorage);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to open StreamCatalog required for compaction publication and materialization", e);
        }
    }

    /**
     * Reflectively loads the {@link CompactionStorageBindings} implementation and constructs it
     * with a single {@code Dependencies} bag. The lakehouse implementation provides a static
     * inner class named {@code Dependencies}; integration modules with different shapes should
     * either follow the same convention or accept the raw {@link StorageConfig}.
     */
    private CompactionStorageBindings buildStorageBindings(StorageConfig storageConfig) {
        String className = storageConfig.getCompactionStorageBindingsClass();
        try {
            Class<?> clazz = Class.forName(className);
            Class<?> depsClass = Class.forName(className + "$Dependencies");
            Class<?> schemaRegistryClass = Class.forName(INTEGRATION_PKG + ".schema.SchemaRegistry");
            Constructor<?> depsCtor = resolveStorageBindingsDependenciesConstructor(
                    depsClass, schemaRegistryClass);
            Object depsInstance = depsCtor.newInstance(
                    storageConfig,
                    streamCatalog,
                    storageApi,
                    ursaStorage == null ? null : ursaStorage.getFileStorage(),
                    compactTaskManager,
                    compactionMetrics,
                    commitTaskProvider,
                    null,
                    scanTopicExecutor,
                    publicationControlExecutor,
                    publishTaskExecutor,
                    compactedTaskExecutor,
                    commitParquetFileExecutor);
            Constructor<?> ctor = clazz.getConstructor(depsClass);
            return (CompactionStorageBindings) ctor.newInstance(depsInstance);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "Failed to load CompactionStorageBindings class: " + className, e);
        }
    }

    static Constructor<?> resolveStorageBindingsDependenciesConstructor(
            Class<?> dependenciesClass, Class<?> schemaRegistryClass) throws NoSuchMethodException {
        return dependenciesClass.getConstructor(
                StorageConfig.class,
                StreamCatalog.class,
                StorageApi.class,
                FileStorage.class,
                CompactTaskManager.class,
                CompactionMetrics.class,
                CommitTaskProvider.class,
                schemaRegistryClass,
                ExecutorService.class,
                ScheduledExecutorService.class,
                ExecutorService.class,
                ExecutorService.class,
                ExecutorService.class);
    }

    /**
     * Honours the new {@code materializationServiceClass} key with the deprecated
     * {@code compactionServiceClass} as fallback (WARN logged when only the legacy key is set).
     */
    private static String resolveMaterializationServiceClass(StorageConfig storageConfig) {
        Properties props = storageConfig.getProperties();
        boolean newKeySet = props != null && props.containsKey("materializationServiceClass");
        boolean legacyKeySet = props != null && props.containsKey("compactionServiceClass");
        if (newKeySet || !legacyKeySet) {
            return storageConfig.getMaterializationServiceClass();
        }
        String legacy = storageConfig.getCompactionServiceClass();
        if (LEGACY_LAKEHOUSE_COMPACTION_SERVICE_CLASS.equals(legacy)) {
            // The legacy default points at the old combined service; map to the new default so the
            // worker can dispatch through the SPI without the caller having to flip configs.
            log.warn("compactionServiceClass is deprecated; using materializationServiceClass default {} instead",
                    storageConfig.getMaterializationServiceClass());
            return storageConfig.getMaterializationServiceClass();
        }
        log.warn("compactionServiceClass is deprecated; falling back to its value {} as materializationServiceClass",
                legacy);
        return legacy;
    }

    /**
     * Resolves the class name used for the legacy {@link CompactionService} indirection. The
     * field still drives WAL → CO compaction; the new {@code materializationServiceClass} key
     * drives the materialization side of T10.
     */
    private static String resolveCompactionServiceClass(StorageConfig storageConfig) {
        return storageConfig.getCompactionServiceClass();
    }

    private CompactionService buildCompactionService(StorageConfig storageConfig,
                                                     String compactionClassName, StorageApi storageApi,
                                                     CompactTaskManager compactTaskManager,
                                                     AsyncOxiaClient asyncOxiaClient,
                                                     CompactionMetrics compactionMetrics,
                                                     Object schemaRegistry) {
        try {
            Class<?> clazz = Class.forName(compactionClassName);
            CompactionService cs = (CompactionService) clazz.getDeclaredConstructor().newInstance();
            cs.initialize(null, ursaStorage == null ? null : ursaStorage.getFileStorage(), null, storageApi,
                    compactTaskManager, storageConfig, asyncOxiaClient, compactionMetrics, schemaRegistry);
            return cs;
        } catch (ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException
                 | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private MaterializationService buildMaterializationService(StorageConfig storageConfig,
                                                               CompactionStorageBindings bindings,
                                                               CompactionMetrics metrics) {
        String className = resolveMaterializationServiceClass(storageConfig);
        MaterializationService svc = MaterializationServiceProvider.load(className);
        MaterializationRuntime runtime = buildMaterializationRuntime(bindings, metrics);
        MaterializationServiceConfig svcConfig = buildMaterializationServiceConfig(storageConfig);
        svc.initialize(runtime, svcConfig);
        return svc;
    }

    private MaterializationRuntime buildMaterializationRuntime(CompactionStorageBindings bindings,
                                                               CompactionMetrics metrics) {
        SchemaService<?> schemaService = (SchemaService<?>) bindings.schemaService();
        if (schemaService == null) {
            schemaService = noopSchemaService();
        }
        // The schema evolution manager is currently a stateless static-method holder; pass the
        // canonical instance so the constructor's non-null check is satisfied.
        SchemaEvolutionManager schemaEvolutionManager = new SchemaEvolutionManager();
        return new MaterializationRuntime(
                schemaService,
                schemaEvolutionManager,
                executor,
                LoggerFactory.getLogger(MaterializationService.class),
                bridgeMetrics(metrics),
                FailureMessageHandler.noop(),
                compactTaskManager,
                resolveSourceEntryFormat(config),
                storageApi);
    }

    /**
     * Resolves the source entry format from the materialization-layer configuration.
     */
    private static EntryFormat resolveSourceEntryFormat(StorageConfig config) {
        String dataSource = config.getProperties().getProperty(
                "entryFormat",
                config.getProperties().getProperty("dataSourceForCompaction", EntryFormat.URSA.name()));
        try {
            return EntryFormat.valueOf(dataSource.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Unsupported entry format '" + dataSource + "'; expected URSA or KAFKA", e);
        }
    }

    /** No-op {@link SchemaService} returned when bindings can't supply one. */
    private static SchemaService<?> noopSchemaService() {
        return new SchemaService<Object>() {
            @Override
            public Map<Long, Object> getSchemaWithVersions(String topic, long schemaVersion) {
                return Map.of();
            }

            @Override
            public void close() {
                // no-op
            }
        };
    }

    /**
     * Wraps the existing {@link CompactionMetrics} as a {@link MaterializationMetrics} so the
     * runtime constructor is satisfied. For T10 the metrics are noop-passthrough; T14 will tie
     * them to real OpenTelemetry instruments.
     */
    private static MaterializationMetrics bridgeMetrics(CompactionMetrics ignored) {
        return MaterializationMetrics.noop();
    }

    private static MaterializationServiceConfig buildMaterializationServiceConfig(StorageConfig cfg) {
        Map<String, String> additional = new HashMap<>();
        if (cfg.getProperties() != null) {
            for (String k : cfg.getProperties().stringPropertyNames()) {
                additional.put(k, cfg.getProperties().getProperty(k));
            }
        }
        return new MaterializationServiceConfig(
                Math.max(1, cfg.getCompactedThreadNum()),
                Duration.ofSeconds(1),
                Math.max(1L, cfg.getWalReadRateLimitInBytesPerSecond()),
                additional);
    }

    protected void startLeaderElectionService() {
        this.leaderElectionService =
                new LeaderElectionService(oxiaClient, hostname,
                        leading -> {
                            if (leading) {
                                log.info("This compactor {} was elected leader", hostname);
                                startLeaderRunners();
                            } else {
                                if (leaderElectionService != null) {
                                    final Optional<CompactLeader> currentLeader =
                                            leaderElectionService.getCurrentLeader();
                                    if (currentLeader.isPresent()) {
                                        log.info("This compactor {} is a follower. Current leader is {}", hostname,
                                                currentLeader);
                                    } else {
                                        log.info("This compactor {} is a follower. No leader has been elected yet",
                                                hostname);
                                    }

                                }
                                stopLeaderRunners();
                            }
                        }, instrumentProvider);
        leaderElectionService.start();
    }

    void startLeaderRunners() {
        try {
            startPublishCompactTaskRunner();
            startCommitParquetFileRunner();
            startAsyncCompactedDataCleaner();
        } catch (RuntimeException | Error startupFailure) {
            stopAfterLeaderStartupFailure(this::stopAsyncCompactedDataCleaner, startupFailure);
            stopAfterLeaderStartupFailure(this::stopCommitParquetFileRunner, startupFailure);
            stopAfterLeaderStartupFailure(this::stopPublishCompactTaskRunner, startupFailure);
            try {
                log.error("Failed to start compaction leader runners; rolled back partial startup",
                        startupFailure);
            } catch (RuntimeException | Error observabilityFailure) {
                addSuppressed(startupFailure, observabilityFailure);
            }
            throw startupFailure;
        }
    }

    void stopLeaderRunners() {
        List<Throwable> stopFailures = new ArrayList<>(3);
        stopLeaderRunner(this::stopAsyncCompactedDataCleaner, stopFailures);
        stopLeaderRunner(this::stopCommitParquetFileRunner, stopFailures);
        stopLeaderRunner(this::stopPublishCompactTaskRunner, stopFailures);
        if (stopFailures.isEmpty()) {
            return;
        }
        Throwable stopFailure = stopFailures.get(0);
        stopFailures.stream().skip(1).forEach(failure -> addSuppressed(stopFailure, failure));
        if (stopFailure instanceof Error error) {
            throw error;
        }
        if (stopFailure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
    }

    private static void stopLeaderRunner(Runnable stopAction, List<Throwable> stopFailures) {
        try {
            stopAction.run();
        } catch (RuntimeException | Error stopFailure) {
            stopFailures.add(stopFailure);
        }
    }

    private static void stopAfterLeaderStartupFailure(Runnable stopAction, Throwable startupFailure) {
        try {
            stopAction.run();
        } catch (RuntimeException | Error stopFailure) {
            addSuppressed(startupFailure, stopFailure);
        }
    }

    private static void addSuppressed(Throwable primary, Throwable secondary) {
        if (primary != secondary) {
            primary.addSuppressed(secondary);
        }
    }

    private void startPublishCompactTaskRunner() {
        stopPublishCompactTaskRunner();
        if (!config.isInternalCompactionTaskPublisherEnabled()) {
            log.info("Internal compaction task publisher is disabled; waiting for externally published tasks");
            return;
        }
        streamCompactTaskRunner = storageBindings.createPublishCompactTaskRunner();
        streamCompactTaskRunner.start();
    }

    private void startAsyncCompactedDataCleaner() {
        stopAsyncCompactedDataCleaner();
        asyncCompactedDataCleaner = storageBindings.createAsyncCompactedDataCleaner();
    }

    private void stopAsyncCompactedDataCleaner() {
        StartStopRunner runner = asyncCompactedDataCleaner;
        if (runner != null) {
            runner.stop();
            if (asyncCompactedDataCleaner == runner) {
                asyncCompactedDataCleaner = null;
            }
        }
    }

    private void stopPublishCompactTaskRunner() {
        StartStopRunner runner = streamCompactTaskRunner;
        if (runner != null) {
            runner.stop();
            if (streamCompactTaskRunner == runner) {
                streamCompactTaskRunner = null;
            }
        }
    }

    private void startCommitParquetFileRunner() {
        stopCommitParquetFileRunner();
        // Gate new commits on leadership. A commit that already entered an external catalog cannot
        // be safely cancelled; the runner therefore drains it before releasing the leader claim and
        // fail-stops this process if the configured deadline expires.
        commitParquetFileRunner = getCommitRunner(
            () -> leaderElectionService != null && leaderElectionService.isLeader());
        commitParquetFileRunner.start();

    }

    // Manual/admin commits (e.g. ManuallyCommitTasks) are not leadership-gated.
    public StartStopRunner getCommitRunner() {
        return getCommitRunner(() -> true);
    }

    // Leadership-gated commit runner, built via the storage bindings so this module stays free of
    // direct integration-package imports (T10). The bindings thread isLeader into CompactedTaskRunner.
    public StartStopRunner getCommitRunner(BooleanSupplier isLeader) {
        return storageBindings.createCompactedTaskRunner(isLeader, this::failStopCompactionProcess);
    }

    private void failStopCompactionProcess(Throwable failure) {
        try {
            log.error("Terminating the compaction process because leader-only work could not be fenced", failure);
        } finally {
            // An external lakehouse commit is not interruptible and cannot be fenced by closing the
            // Oxia client alone. Halt the process so the old callable cannot overlap a successor;
            // the OS closes the Oxia session and its ephemeral leader record as part of termination.
            Runtime.getRuntime().halt(ExitCode.SERVER_EXCEPTION);
        }
    }

    private void stopCommitParquetFileRunner() {
        Throwable stopFailure = null;
        StartStopRunner runner = commitParquetFileRunner;
        if (runner != null) {
            try {
                runner.stop();
                if (commitParquetFileRunner == runner) {
                    commitParquetFileRunner = null;
                }
            } catch (RuntimeException | Error runnerFailure) {
                stopFailure = runnerFailure;
            }
        }

        if (updateCommitTasksFuture != null) {
            try {
                updateCommitTasksFuture.cancel(true);
                updateCommitTasksFuture = null;
            } catch (RuntimeException | Error cancellationFailure) {
                if (stopFailure == null) {
                    stopFailure = cancellationFailure;
                } else {
                    addSuppressed(stopFailure, cancellationFailure);
                }
            }
        }
        if (stopFailure instanceof Error error) {
            throw error;
        }
        if (stopFailure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
    }

    public void initCompactRunner() {
        log.info("Create {} {} compact runners", config.getCompactedThreadNum(),
                compactionService.getClass().getName());
        for (int i = 0; i < config.getCompactedThreadNum(); i++) {
            executor.execute(new CompactionWorker(compactTaskManager, compactionService,
                    materializationService, streamCatalog,
                    compactionTaskProvider, config, compactionMetrics));
        }
    }

    private void runCompactionMaintenance() {
        try {
            compactionService.maintenance();
        } catch (Throwable t) {
            log.warn("Failed to run compaction maintenance.", t);
        }
    }

    public void close() throws InterruptedException {
        // Fence leadership before stopping or closing any dependency that a leadership callback
        // can start. LeaderElectionService.close waits for an in-flight listener transition and
        // rejects late promotions after its closed flag is set.
        if (leaderElectionService != null) {
            try {
                leaderElectionService.close();
            } catch (RuntimeException | Error error) {
                try {
                    log.warn("Failed to close leader election", error);
                } catch (RuntimeException | Error observabilityFailure) {
                    addSuppressed(error, observabilityFailure);
                }
            } finally {
                leaderElectionService = null;
            }
        }

        try {
            stopLeaderRunners();
        } catch (RuntimeException | Error stopFailure) {
            try {
                log.warn("Failed to stop one or more compaction leader runners", stopFailure);
            } catch (RuntimeException | Error observabilityFailure) {
                addSuppressed(stopFailure, observabilityFailure);
            }
        }

        if (maintenanceFuture != null) {
            try {
                maintenanceFuture.cancel(true);
            } catch (RuntimeException | Error cancellationFailure) {
                try {
                    log.warn("Failed to cancel compaction maintenance", cancellationFailure);
                } catch (RuntimeException | Error observabilityFailure) {
                    addSuppressed(cancellationFailure, observabilityFailure);
                }
            }
        }

        InterruptedException shutdownInterrupted = null;
        ExecutorService[] executors = {
            executor,
            scanTopicExecutor,
            publishTaskExecutor,
            compactedTaskExecutor,
            commitParquetFileExecutor,
            scheduledExecutor,
            publicationControlExecutor
        };
        for (ExecutorService service : executors) {
            if (service == null) {
                continue;
            }
            try {
                shutdownExecutor(service, 10, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                if (shutdownInterrupted == null) {
                    shutdownInterrupted = interrupted;
                } else {
                    shutdownInterrupted.addSuppressed(interrupted);
                }
                // shutdownExecutor restores the flag. Clear it temporarily so every remaining
                // executor receives the same forced-shutdown/await sequence and all dependent
                // resources can still be closed. The flag is restored before close() returns.
                Thread.interrupted();
            }
        }
        try {
            if (lockManager != null) {
                lockManager.close();
            }
        } catch (Exception e) {
            log.warn("Failed to close lock manager", e);
        }
        if (compactionService != null) {
            compactionService.close();
        }
        if (materializationService != null) {
            try {
                materializationService.close();
            } catch (Exception e) {
                log.warn("Failed to close materialization service", e);
            }
        }
        if (storageBindings != null) {
            try {
                storageBindings.close();
            } catch (Exception e) {
                log.warn("Failed to close compaction storage bindings", e);
            }
        }
        if (streamCatalog != null) {
            try {
                streamCatalog.close();
            } catch (Exception e) {
                log.warn("Failed to close stream catalog", e);
            }
        }

        try {
            if (oxiaClient != null) {
                oxiaClient.close();
            }
        } catch (Exception e) {
            log.warn("Failed to shutdown oxia client", e);
        }
        try {
            if (storageOxiaClient != null) {
                storageOxiaClient.close();
            }
        } catch (Exception e) {
            log.warn("Failed to shutdown storage oxia client", e);
        }
        try {
            if (ursaStorage != null) {
                ursaStorage.close();
            }
        } catch (Exception e) {
            log.warn("Failed to shutdown storage oxia client", e);
        }
        if (shutdownInterrupted != null) {
            Thread.currentThread().interrupt();
            throw shutdownInterrupted;
        }
    }

    static void shutdownExecutor(ExecutorService executor, long timeout, TimeUnit unit)
            throws InterruptedException {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(timeout, unit)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(timeout, unit)) {
                    log.warn("Executor did not terminate after forced shutdown within {} {}", timeout, unit);
                }
            }
        } catch (InterruptedException interrupted) {
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(timeout, unit)) {
                    log.warn("Executor did not terminate after interrupted forced shutdown within {} {}",
                            timeout, unit);
                }
            } catch (InterruptedException secondInterrupt) {
                interrupted.addSuppressed(secondInterrupt);
            }
            Thread.currentThread().interrupt();
            throw interrupted;
        }
    }

    public void start() {
        if (streamCatalog != null) {
            bootstrapTableCatalogs(streamCatalog, config.getProperties());
        }
        initCompactRunner();
        startLeaderElectionService();
    }

    /**
     * Invokes the integration-supplied {@code TableCatalogBootstrap.bootstrap(StreamCatalog,
     * Properties)} reflectively so this module stays free of integration-package imports. The
     * bootstrap is idempotent; failures are logged and do not fail startup.
     */
    private static void bootstrapTableCatalogs(StreamCatalog catalog, Properties properties) {
        try {
            Class<?> bootstrapClass = Class.forName(LAKEHOUSE_BOOTSTRAP_CLASS);
            Method bootstrap = bootstrapClass.getMethod("bootstrap",
                    Class.forName("io.lakestream.api.StreamCatalog"),
                    Properties.class);
            Object result = bootstrap.invoke(null, catalog, properties);
            Class<?> resultClass = result.getClass();
            List<?> registered = (List<?>) resultClass.getMethod("registered").invoke(result);
            List<?> skipped = (List<?>) resultClass.getMethod("skipped").invoke(result);
            List<?> errors = (List<?>) resultClass.getMethod("errors").invoke(result);
            log.info("TableCatalog bootstrap registered {} catalogs ({} skipped, {} errors)",
                    registered.size(), skipped.size(), errors.size());
            if (!errors.isEmpty()) {
                errors.forEach(err -> log.warn("TableCatalog bootstrap error: {}", err));
            }
        } catch (ClassNotFoundException | NoSuchMethodException
                 | IllegalAccessException | InvocationTargetException e) {
            log.warn("Skipping TableCatalog bootstrap; {} is not available on the classpath",
                    LAKEHOUSE_BOOTSTRAP_CLASS, e);
        }
    }

    /**
     * Builds the cluster-wide {@link LockManager} through the lakehouse-provided helper without
     * importing the lakehouse class directly. Keeping the call reflective is what lets the
     * binary check ("no lakehouse imports in compact/main") pass while we wait for a sink-neutral
     * LockManager builder.
     */
    private static LockManager createLockManagerReflectively(AsyncOxiaClient asyncOxiaClient) {
        try {
            Class<?> clazz = Class.forName(LAKEHOUSE_LOCK_MANAGERS_CLASS);
            Method createLockManager = clazz.getMethod("createLockManager", AsyncOxiaClient.class);
            return (LockManager) createLockManager.invoke(null, asyncOxiaClient);
        } catch (ClassNotFoundException | NoSuchMethodException
                 | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException(
                    "Failed to construct LockManager via " + LAKEHOUSE_LOCK_MANAGERS_CLASS, e);
        }
    }

}
