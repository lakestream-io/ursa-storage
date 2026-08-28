/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import io.lakestream.api.Stream;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.materialization.EvolutionPolicy;
import io.lakestream.api.materialization.ResolvedMaterialization;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.ExceptionWithCode;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.v2.AbstractLakehouseWriter;
import io.lakestream.ursa.lakehouse.v2.IWriteResult;
import io.lakestream.ursa.lakehouse.v2.LakehouseFactory;
import io.lakestream.ursa.lakehouse.v2.LakehouseTableMaterializer;
import io.lakestream.ursa.lakehouse.v2.TableCatalogBootstrap;
import io.lakestream.ursa.lakehouse.v2.io.parquet.ParquetWriteResult;
import io.lakestream.ursa.materialization.CommitResult;
import io.lakestream.ursa.materialization.MaterializationContext;
import io.lakestream.ursa.materialization.MaterializationException;
import io.lakestream.ursa.materialization.MaterializationMetrics;
import io.lakestream.ursa.materialization.MaterializationRuntime;
import io.lakestream.ursa.materialization.MaterializationService;
import io.lakestream.ursa.materialization.MaterializationServiceConfig;
import io.lakestream.ursa.materialization.MaterializationTask;
import io.lakestream.ursa.materialization.TableMaterializer;
import io.lakestream.ursa.materialization.TableMaterializerFactory;
import io.lakestream.ursa.materialization.serde.EntryFormat;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.LakehouseEntryMetadata;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.StorageApi;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

/**
 * Lakehouse-backed implementation of the {@link MaterializationService} SPI.
 *
 * <p>T10 wired this service end to end:
 * <ul>
 *   <li>Factories are discovered via {@link ServiceLoader} on initialise.</li>
 *   <li>{@link #materialize(MaterializationTask)} looks up the {@link TableMaterializerFactory} for
 *       the stream's effective {@link TableCatalogType} and asks it to
 *       {@link TableMaterializerFactory#create create} a FRESH {@link TableMaterializer} for the task
 *       (plus, when SBT is enabled, a managed Compacted-Object materializer). Materializers are
 *       single-use (commit()/close() are terminal) and never cached — a partitioned topic's partitions
 *       are compacted concurrently under one stream identity, so a per-stream cache would share one
 *       single-use materializer across threads.</li>
 *   <li>Each record is dispatched to every materializer's {@link TableMaterializer#write} with a
 *       {@link MaterializationContext} that sources the per-record timestamp from
 *       {@link LakehouseEntryMetadata} when present.</li>
 *   <li>{@link TableMaterializer#commit()} runs at the end of every task; the compaction task is then
 *       persisted (group commit) or retired from the committed materializers' write results.</li>
 *   <li>Opened stream handles are retained while a materializer uses them and closed after
 *       replacement, invalidation, or service shutdown.</li>
 * </ul>
 *
 * <p>The "stream handle" the factory needs is now supplied via
 * {@link #registerActiveStream(StreamIdentifier, Stream)}; the orchestrator
 * (T10's {@code CompactionWorker}) calls this once per stream before submitting
 * the first task. When the orchestrator does not supply a handle the service
 * raises {@link MaterializationException} so the failure surfaces through the
 * code-aware retry pipeline.
 */
@Slf4j
public class LakehouseMaterializationService implements MaterializationService {

    private static final long RETAINED_STREAM_CLOSE_RETRY_INITIAL_DELAY_MILLIS = 10L;
    private static final long RETAINED_STREAM_CLOSE_RETRY_MAX_DELAY_MILLIS = 1_000L;

    private MaterializationRuntime runtime;
    private MaterializationServiceConfig config;
    private final Map<TableCatalogType, TableMaterializerFactory> factories = new ConcurrentHashMap<>();
    private final Map<StreamIdentifier, RetainedStream> activeStreams = new ConcurrentHashMap<>();
    private final Set<RetainedStream> retiredStreams = ConcurrentHashMap.newKeySet();
    private final Object activeStreamsLifecycleLock = new Object();
    private final ScheduledExecutorService retainedStreamCloseRetryExecutor =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "lakehouse-retained-stream-close-retry");
                thread.setDaemon(true);
                return thread;
            });
    private volatile boolean closed;

    /**
     * Supplies entry readers (Ursa WAL or Kafka). Built lazily from config on first use;
     * package-private setter lets tests inject a stub reader so the read loop is unit-testable.
     */
    private volatile EntryReaderProvider entryReaderProvider;
    /**
     * Source-aware entry-reader factories, cached per {@link EntryFormat}. The source is a per-task
     * {@code entryFormat} property (URSA / KAFKA), and
     * partitions of different types can be compacted concurrently by the shared service, so a single
     * cached factory would read a Kafka task off the WAL (or vice versa) and fail with
     * {@code NoSuchOffsetException}. Keyed by format so each source gets the right reader.
     */
    private final Map<EntryFormat, EntryProcessFactory> entryProcessFactories = new ConcurrentHashMap<>();
    /**
     * Lazily-built factory for the SBT (managed) writer. The managed {@code LakehouseWriter}
     * compacts the same WAL entries into topic-grouped parquet "Compacted Objects" — the stream's own
     * data — independently of the external (SDT) sink. Always-on for Ursa; gated by {@code sbtEnabled}.
     */
    private volatile LakehouseFactory lakehouseFactory;

    /**
     * Public no-arg constructor used by
     * {@link io.lakestream.ursa.materialization.MaterializationServiceProvider}.
     */
    public LakehouseMaterializationService() {
    }

    @Override
    public synchronized void initialize(MaterializationRuntime runtime, MaterializationServiceConfig config) {
        ensureOpen();
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.config = Objects.requireNonNull(config, "config");

        // Discover factories via ServiceLoader. Each TableCatalogType has at most one factory;
        // duplicate registrations log a warning and the first-loaded factory wins.
        ServiceLoader<TableMaterializerFactory> loader = ServiceLoader.load(TableMaterializerFactory.class);
        Iterator<TableMaterializerFactory> iterator = loader.iterator();
        while (iterator.hasNext()) {
            try {
                TableMaterializerFactory factory = iterator.next();
                TableMaterializerFactory existing = factories.putIfAbsent(factory.catalogType(), factory);
                if (existing != null) {
                    log.warn("Multiple TableMaterializerFactory implementations registered for {}: "
                                    + "keeping {} and ignoring {}",
                            factory.catalogType(), existing.getClass().getName(),
                            factory.getClass().getName());
                }
            } catch (Throwable t) {
                log.warn("Failed to instantiate a TableMaterializerFactory via ServiceLoader; skipping", t);
            }
        }
        log.info("LakehouseMaterializationService initialized with {} factories: {}",
                factories.size(), factories.keySet());
    }

    @Override
    public Optional<ResolvedMaterialization> resolveFromTaskProperties(
            StreamIdentifier streamId, String topic, Map<String, String> taskProperties) {
        ensureOpen();
        // Compatibility: synthesize a catalog + policy from the deployment config merged with the task's
        // properties (legacy DynamicConfigs + catalog config), so a stream with no stream/namespace/
        // cluster policy still materializes when the task carries the materialization config. Task
        // properties override the deployment defaults.
        Properties merged = new Properties();
        if (config != null) {
            merged.putAll(config.additionalProperties());
        }
        if (taskProperties != null) {
            merged.putAll(taskProperties);
        }
        return TableCatalogBootstrap.resolveFromProperties(merged, streamId);
    }

    @Override
    public void materialize(MaterializationTask task) {
        Objects.requireNonNull(task, "task");
        ensureOpen();
        if (runtime == null) {
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                    "LakehouseMaterializationService not initialized");
        }

        ResolvedMaterialization resolved = task.resolvedMaterialization();
        TableCatalog catalog = resolved.catalog();
        TableCatalogType catalogType = catalog.type();
        // A NONE catalog marks a managed-only (SBT / Ursa-protocol) materialization: SDT is disabled and
        // no external table is configured, so only the internal managed Compacted-Object writer runs.
        // There is no external factory for it. Any other type must have a registered factory for the
        // external (SDT) sink.
        boolean externalSink = catalogType != TableCatalogType.NONE;
        TableMaterializerFactory factory = externalSink ? factories.get(catalogType) : null;
        if (externalSink && factory == null) {
            String reason = "No TableMaterializerFactory registered for catalog type " + catalogType;
            runtime.metrics().recordSchemaEvolutionRejected(
                    catalog.name(), catalogType, task.stream(), reason);
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR, reason);
        }

        log.info("Materializing [{},{}) of stream {} into catalog {} ({}) table {}.{}",
            task.startOffset(), task.endOffset(), task.stream().fullName(), catalog.name(), catalogType,
            resolved.tableIdentifier().namespace(), resolved.tableIdentifier().name());

        // Thread the source entry format (URSA / KAFKA) from the originating compaction task
        // into the runtime so the sink picks the source-aware encoder. Also carry the
        // task's properties (legacy DynamicConfigs: sdtCatalogName, identifierFields, upsertMode,
        // baseSchemaVersion, …) so the sink stays compatible with task-property-driven materialization.
        final EntryFormat entryFormat = entryFormatOf(task);
        final Map<String, String> sourceTaskProperties = sourceTaskProperties(task);
        final String sourceReadTopic = KafkaEntryProcessFactory.resolveSourceTopic(
                task.sourceTopic(), entryFormat, sourceTaskProperties);
        final MaterializationRuntime writerRuntime = runtime.withEntryFormat(entryFormat)
                .withTaskProperties(sourceTaskProperties);
        // A TableMaterializer is single-use (commit()/close() are terminal), so build a fresh one per
        // task via the factory — never cache it. A partitioned topic's partitions are compacted by
        // separate worker threads concurrently but share one stream identity (namespace+name, no
        // partition); a per-stream cache would hand one single-use materializer to multiple threads,
        // corrupting the writer ("Index N out of bounds" in the Parquet dictionary) and tripping
        // "write() after commit()".
        // Everything is a TableMaterializer: the SDT sink (external Iceberg/Delta or inline ClickHouse)
        // plus — when SBT is enabled — the managed Compacted-Object writer wrapped as a materializer.
        // writeAndCommit fans the single WAL read pass out to all of them, so a task can sink to
        // multiple destinations at once; task completion is driven from their write results afterwards.
        // A managed-only (NONE) task has no external sink, so only the managed writer is built.
        List<TableMaterializer<?>> materializers = new ArrayList<>();
        ActiveStreamLease streamLease = null;
        boolean committed = false;
        try {
            if (externalSink) {
                // Hold a lease for the entire materialization. Replacing or invalidating the
                // registered handle retires it immediately, but cannot close it under an in-flight
                // materializer.
                streamLease = acquireActiveStream(task.stream());
                if (streamLease == null) {
                    throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                            "No active Stream handle registered for " + task.stream().fullName()
                                    + "; call registerActiveStream() before materialize()");
                }
                TableMaterializer<?> materializer = factory.create(
                        resolved.effectivePolicy(), catalog, streamLease.stream(), writerRuntime);
                materializers.add(materializer);
                // factory.create() may perform remote setup. If shutdown won the race while it was
                // in flight, keep the result in the local list so finally closes it, then stop before
                // opening a reader or writing data.
                ensureOpen();
            }
            buildManagedMaterializer(task, task.sourceTopic(), entryFormat).ifPresent(materializers::add);
            ensureOpen();
            if (materializers.isEmpty()) {
                throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                        "No materializer available for stream " + task.stream().fullName()
                                + " (managed-only catalog but the managed writer was disabled or skipped)");
            }
            CommitResult result = writeAndCommit(materializers, task, sourceReadTopic, sourceTaskProperties,
                    entryFormat);
            committed = true;
            runtime.metrics().recordWritten(catalog.name(), catalogType, task.stream());
            log.info("Committed task {} in MaterializationService for stream {}: {}",
                task.sourceTopic(), task.stream().fullName(), result);
            // Persist (group-commit) or retire the compaction task from the committed materializers' write results.
            completeTask(materializers, task);
        } catch (MaterializationException e) {
            runtime.metrics().recordSchemaEvolutionRejected(
                    catalog.name(), catalogType, task.stream(), e.getMessage());
            throw e;
        } catch (RuntimeException e) {
            runtime.metrics().recordSchemaEvolutionRejected(
                    catalog.name(), catalogType, task.stream(), e.getMessage());
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                    "Materialization failed for stream " + task.stream().fullName(), e);
        } finally {
            // Single-use materializers, not shared. On success writeAndCommit already committed (and
            // closed) them; on the failure path close any still-open to release resources. close()
            // after commit() is a no-op.
            if (!committed) {
                for (TableMaterializer<?> m : materializers) {
                    try {
                        m.close();
                    } catch (RuntimeException ex) {
                        log.warn("Failed to close materializer for stream {} after a failed task",
                                task.stream().fullName(), ex);
                    }
                }
            }
            if (streamLease != null) {
                streamLease.close();
            }
        }
    }

    /**
     * Reads the source entries for the task's offset range ONCE (via the
     * source-aware reader factory) and fans each raw {@link GenericEntry} out to every supplied
     * {@link TableMaterializer}, then commits them all. Every destination — the SDT sink and the SBT
     * managed Compacted-Object writer alike — is just a materializer, so a single task can sink to
     * multiple destinations from one read pass. Does NOT complete the compaction task; the caller
     * ({@link #materialize}) drives {@link #completeTask} from the committed materializers.
     *
     * @return the aggregated commit outcome (summed records/bytes) across all materializers
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private CommitResult writeAndCommit(List<TableMaterializer<?>> materializers, MaterializationTask task,
                                        String sourceTopic, Map<String, String> sourceTaskProperties,
                                        EntryFormat entryFormat) {
        ensureOpen();
        long offset = task.startOffset();
        IEntryReader reader;
        try {
            reader = openEntryReader(entryFormat,
                    sourceTopic, task.streamId(), task.startOffset(), task.endOffset(), sourceTaskProperties);
        } catch (ExceptionWithCode e) {
            throw new MaterializationException(e.getExceptionCode(),
                    "Failed to open entry reader for " + sourceTopic
                            + " in [" + task.startOffset() + "," + task.endOffset() + ")", e);
        } catch (Exception e) {
            throw new MaterializationException(ExceptionCode.SOURCE_READ_ERROR,
                    "Failed to open entry reader for " + sourceTopic
                            + " in [" + task.startOffset() + "," + task.endOffset() + ")", e);
        }
        try {
            GenericEntry entry;
            while (true) {
                ensureOpen();
                entry = reader.read();
                if (entry == null) {
                    ensureOpen();
                    break;
                }
                try {
                    // reader.read() transfers ownership of every non-null entry. Establish the
                    // release boundary before any shutdown fence, metadata extraction, or context
                    // construction can fail.
                    ensureOpen();
                    long timestamp = extractTimestamp(entry);
                    MaterializationContext ctx = new MaterializationContext(
                            task.stream(),
                            offset++,
                            timestamp,
                            Optional.empty(),
                            Map.of());
                    // TableMaterializer.write takes ownership even when it throws. Each materializer
                    // therefore gets its own retained duplicate immediately before the call. The
                    // original remains owned here and is released once in finally.
                    for (TableMaterializer<?> materializer : materializers) {
                        GenericEntry forWrite = new GenericEntry(
                                entry.entry().retainedDuplicate(), entry.metadata());
                        ((TableMaterializer) materializer).write(forWrite, ctx);
                    }
                } finally {
                    if (entry.entry().payload() != null) {
                        entry.entry().payload().release();
                    }
                }
            }
        } catch (MaterializationException e) {
            throw e;
        } catch (ExceptionWithCode e) {
            throw new MaterializationException(e.getExceptionCode(),
                    "Failed to read source entries for " + sourceTopic
                            + " in [" + task.startOffset() + "," + task.endOffset() + ")", e);
        } catch (Exception e) {
            throw new MaterializationException(ExceptionCode.SOURCE_READ_ERROR,
                    "Failed to read source entries for " + sourceTopic
                            + " in [" + task.startOffset() + "," + task.endOffset() + ")", e);
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (Exception e) {
                log.warn("Failed to close entry reader for {}", sourceTopic, e);
            }
        }
        // Commit every materializer (each flushes its files / performs its inline commit) and aggregate
        // the outcome. Each commit() is terminal and retains its write results for completeTask().
        long totalRecords = 0;
        long totalBytes = 0;
        Map<String, String> metadata = new LinkedHashMap<>();
        for (TableMaterializer<?> materializer : materializers) {
            ensureOpen();
            CommitResult result = materializer.commit();
            if (result != null) {
                totalRecords += result.recordsCommitted();
                totalBytes += result.bytesCommitted();
                metadata.putAll(result.sinkMetadata());
            }
        }
        return new CommitResult(totalRecords, totalBytes, metadata);
    }

    /**
     * Builds the SBT (managed) materializer for the task's topic, or {@link Optional#empty()} when the
     * managed workflow should not run. The managed {@code LakehouseWriter} compacts the WAL into
     * topic-grouped parquet Compacted Objects; it is wrapped as a {@link LakehouseTableMaterializer} so
     * the write path treats SBT and SDT uniformly. Only used in {@code streamTableMode=EXTERNAL} (in
     * MANAGED mode the SDT materializer itself is the managed writer); {@code getManagedWriter}
     * additionally returns empty when {@code sbtEnabled} is false or {@code skipManagedWriter} is set.
     */
    private Optional<TableMaterializer<?>> buildManagedMaterializer(MaterializationTask task,
                                                                     String sourceTopic,
                                                                     EntryFormat entryFormat) {
        ensureOpen();
        CompactStreamTask sourceTask = task.sourceTask();
        if (sourceTask == null) {
            // Unit tests drive materialize() directly without a source task; no managed compaction.
            return Optional.empty();
        }
        Properties properties = new Properties();
        if (config != null) {
            properties.putAll(config.additionalProperties());
        }
        LakehouseConfiguration lakehouseConfig = new LakehouseConfiguration(properties);
        LakehouseFactory factory = lakehouseFactory(lakehouseConfig);
        var propertiesForWriter = new HashMap<String, String>();
        if (sourceTask.getProperties() != null) {
            propertiesForWriter.putAll(sourceTask.getProperties());
        }
        propertiesForWriter.put("entryFormat", entryFormat.name());
        EvolutionPolicy evolutionPolicy = evolutionPolicyFor(
                task.resolvedMaterialization().catalog().type());
        Optional<TableMaterializer<?>> materializer = factory.getManagedWriter(sourceTopic, propertiesForWriter)
                // The managed writer is an AbstractLakehouseWriter; wrap it as
                // a materializer with no DLT.
                .map(writer -> new LakehouseTableMaterializer(
                        (AbstractLakehouseWriter) writer, evolutionPolicy, null));
        try {
            ensureOpen();
            return materializer;
        } catch (RuntimeException | Error closedFailure) {
            materializer.ifPresent(opened -> {
                try {
                    opened.close();
                } catch (RuntimeException | Error closeFailure) {
                    closedFailure.addSuppressed(closeFailure);
                }
            });
            throw closedFailure;
        }
    }

    private static EvolutionPolicy evolutionPolicyFor(TableCatalogType catalogType) {
        return switch (catalogType) {
            case DELTA, DELTA_UC -> EvolutionPolicy.forDelta();
            case CLICKHOUSE -> EvolutionPolicy.forClickHouse();
            case ICEBERG, NONE -> EvolutionPolicy.forIceberg();
        };
    }

    private synchronized LakehouseFactory lakehouseFactory(LakehouseConfiguration lakehouseConfig) {
        ensureOpen();
        LakehouseFactory factory = lakehouseFactory;
        if (factory != null) {
            return factory;
        }
        // The managed writer decodes WAL entries with the same source schema service the SDT
        // workflow uses. Read-path metrics are NOOP.
        lakehouseFactory = new LakehouseFactory(
                lakehouseConfig, runtime.schemaService(), InstrumentProvider.NOOP);
        ensureOpen();
        return lakehouseFactory;
    }

    /**
     * Completes the task after both write workflows finish. When either workflow produced file results
     * (SBT managed parquet and/or SDT external Iceberg/Delta files), the task is persisted as
     * {@code COMPACTED} via {@link CompactionTaskCompleter} so the {@code CompactedTaskRunner} applies
     * the batched commit, advances the offload cursor, and deletes the task. When there are NO file
     * results (an inline-commit sink such as ClickHouse with SBT disabled), the data is already
     * committed, so the task is retired directly via {@link #retireInlineCommittedTask}.
     *
     * <p>No-op when no {@link CompactTaskManager} / source task is wired (unit tests).
     */
    private void completeTask(List<TableMaterializer<?>> materializers, MaterializationTask task) {
        CompactTaskManager compactTaskManager = runtime.compactTaskManager();
        CompactStreamTask sourceTask = task.sourceTask();
        if (compactTaskManager == null || sourceTask == null) {
            return;
        }
        // Partition every materializer's write results by type: managed parquet Compacted Objects
        // (ParquetWriteResult — the SBT path, whether from the standalone managed materializer or a
        // MANAGED-mode SDT materializer) vs external Iceberg/Delta files (the SDT path). DLT files come
        // from the external materializer only. Non-Lakehouse sinks (e.g. ClickHouse) commit inline and
        // contribute no IWriteResults.
        List<IWriteResult> managedResults = new ArrayList<>();
        List<IWriteResult> externalResults = new ArrayList<>();
        List<IWriteResult> dltResults = new ArrayList<>();
        for (TableMaterializer<?> materializer : materializers) {
            if (materializer instanceof LakehouseTableMaterializer lakehouse) {
                for (IWriteResult wr : orEmpty(lakehouse.lastWriteResults())) {
                    if (wr instanceof ParquetWriteResult) {
                        managedResults.add(wr);
                    } else {
                        externalResults.add(wr);
                    }
                }
                dltResults.addAll(orEmpty(lakehouse.lastDltWriteResults()));
            }
        }
        if (managedResults.isEmpty() && externalResults.isEmpty() && dltResults.isEmpty()) {
            // Inline-commit sink (e.g. ClickHouse) with SBT disabled: nothing to group-commit.
            log.info("Retiring inline-committed task {} for stream {}: no managed or external write results",
                    sourceTask.getTaskName(), task.stream().fullName());
            retireInlineCommittedTask(task);
            return;
        }
        CompactionTaskCompleter completer =
                new CompactionTaskCompleter(compactTaskManager, managedTableSchemaEvolutionEnabled());
        try {
            completer.completeCompaction(sourceTask, managedResults, externalResults, dltResults);
        } catch (MaterializationException e) {
            throw e;
        } catch (Exception e) {
            throw new MaterializationException(ExceptionCode.LAKEHOUSE_COMMIT_ERROR,
                    "Failed to persist compacted task " + sourceTask.getTaskName()
                            + " for group commit: " + e.getMessage(), e);
        }
    }

    private static List<IWriteResult> orEmpty(List<IWriteResult> results) {
        return results == null ? Collections.emptyList() : results;
    }

    /**
     * Retires the source compaction task for an inline-commit sink. The data is already durably
     * committed by {@code commit()}, so there is no group-commit step: mark the task
     * {@link CompactStreamTask#COMMITTED} and delete it. The {@code CompactionWorker} only dispatches
     * {@link CompactStreamTask#INIT} tasks, so without this the same range would be re-materialized
     * (and re-inserted, since inline APPEND sinks have no dedup) on every tick.
     *
     * <p>No-op when no {@link CompactTaskManager} or source task is wired (e.g. unit tests that drive
     * {@code materialize()} directly).
     */
    private void retireInlineCommittedTask(MaterializationTask task) {
        CompactTaskManager compactTaskManager = runtime.compactTaskManager();
        CompactStreamTask sourceTask = task.sourceTask();
        if (compactTaskManager == null || sourceTask == null) {
            return;
        }
        sourceTask.setStatus(CompactStreamTask.COMMITTED);
        sourceTask.setMessageWrittenToUrsaTime(System.currentTimeMillis());
        sourceTask.setRealStartOffset(sourceTask.getStartOffset());
        sourceTask.setRealEndOffset(sourceTask.getEndOffset());
        // Inline sinks bypass the group-commit runner, which would otherwise advance the offload
        // cursor (AbstractCommitRunner.updateOffloadCursor). Do it here so the WAL can be trimmed.
        try {
            // Persist COMMITTED before deleting so a crash between the two leaves a terminal marker
            // (not a re-runnable INIT task); the CompactedTaskRunner cleans up stray COMMITTED tasks.
            compactTaskManager.updateCompactTask(sourceTask).get();
            compactTaskManager.deleteCompactTask(sourceTask).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MaterializationException(ExceptionCode.LAKEHOUSE_COMMIT_ERROR,
                    "Interrupted while retiring inline-committed task " + sourceTask.getTaskName(), e);
        } catch (ExecutionException e) {
            throw new MaterializationException(ExceptionCode.LAKEHOUSE_COMMIT_ERROR,
                    "Failed to retire inline-committed task " + sourceTask.getTaskName(), e);
        }
    }

    private boolean managedTableSchemaEvolutionEnabled() {
        return config != null && Boolean.parseBoolean(
                config.additionalProperties().getOrDefault("managedTableSchemaEvolutionEnabled", "false"));
    }

    /** Test seam: inject a stub {@link EntryReaderProvider} so the read loop runs without a broker. */
    synchronized void setEntryReaderProvider(EntryReaderProvider provider) {
        ensureOpen();
        this.entryReaderProvider = provider;
    }

    /** Test seam: inject the managed-writer factory without opening a real lakehouse catalog. */
    synchronized void setLakehouseFactory(LakehouseFactory factory) {
        ensureOpen();
        this.lakehouseFactory = factory;
    }

    /**
     * The source {@link EntryFormat} for a task (URSA / KAFKA). This is a per-task property: the reader source must
     * match the task that produced the range. Falls back to the runtime format when a task carries no
     * source task (unit tests that drive {@code materialize()} directly).
     */
    private EntryFormat entryFormatOf(MaterializationTask task) {
        if (task.sourceTask() != null && task.sourceTask().getProperties() != null) {
            String configured = task.sourceTask().getProperties().get("entryFormat");
            if (configured != null && !configured.isBlank()) {
                try {
                    return EntryFormat.valueOf(configured.trim().toUpperCase(Locale.ROOT));
                } catch (IllegalArgumentException e) {
                    throw new MaterializationException(ExceptionCode.SOURCE_CLIENT_ERROR,
                            "Unsupported entry format '" + configured + "'; expected URSA or KAFKA", e);
                }
            }
        }
        return runtime == null || runtime.entryFormat() == null ? EntryFormat.URSA : runtime.entryFormat();
    }

    private static Map<String, String> sourceTaskProperties(MaterializationTask task) {
        if (task.sourceTask() == null || task.sourceTask().getProperties() == null) {
            return Map.of();
        }
        return task.sourceTask().getProperties();
    }

    /**
     * Opens a reader over {@code [start, end)} for the given per-task source {@code format}. A
     * test-injected {@link EntryReaderProvider} (format-agnostic stub) takes precedence; otherwise the
     * cached, source-aware {@link EntryProcessFactory} for {@code format} is used.
     */
    private IEntryReader openEntryReader(EntryFormat format, String topic, long streamId,
                                         long start, long end,
                                         Map<String, String> sourceTaskProperties) throws Exception {
        ensureOpen();
        EntryReaderProvider injected = this.entryReaderProvider;
        if (injected != null) {
            return ensureReaderOpenedBeforeShutdown(injected.create(topic, streamId, start, end));
        }
        Properties properties = new Properties();
        if (config != null) {
            properties.putAll(config.additionalProperties());
        }
        boolean skipMarkerMessages = Boolean.parseBoolean(
                properties.getProperty("skipMarkerMessages", "false"));
        EntryReaderOptions options = new EntryReaderOptions(skipMarkerMessages);
        // Start with a conservative average entry-size estimate.
        EntryProcessFactory factory = entryProcessFactoryFor(format, properties);
        ensureOpen();
        if (sourceTaskProperties == null) {
            return ensureReaderOpenedBeforeShutdown(
                    factory.createEntryReader(topic, streamId, start, end, 1024.0, options));
        }
        return ensureReaderOpenedBeforeShutdown(factory.createEntryReader(
                topic, streamId, start, end, 1024.0, options, sourceTaskProperties));
    }

    private IEntryReader ensureReaderOpenedBeforeShutdown(IEntryReader reader) throws Exception {
        try {
            ensureOpen();
            return reader;
        } catch (RuntimeException | Error closedFailure) {
            try {
                reader.close();
            } catch (Exception | Error closeFailure) {
                closedFailure.addSuppressed(closeFailure);
            }
            throw closedFailure;
        }
    }

    /**
     * Returns the {@link EntryProcessFactory} for {@code format}, building and caching it on first use.
     * A {@code null} format maps to {@link EntryFormat#URSA} (the storage-native default, and a valid map
     * key since {@code ConcurrentHashMap} forbids null keys).
     */
    synchronized EntryProcessFactory entryProcessFactoryFor(EntryFormat format, Properties properties)
            throws Exception {
        ensureOpen();
        EntryFormat key = format == null ? EntryFormat.URSA : format;
        EntryProcessFactory existing = entryProcessFactories.get(key);
        if (existing != null) {
            return existing;
        }
        LakehouseConfiguration lakehouseConfig = new LakehouseConfiguration(properties);
        EntryProcessFactory factory = buildEntryProcessFactory(key, lakehouseConfig, properties);
        try {
            ensureOpen();
            entryProcessFactories.put(key, factory);
            return factory;
        } catch (RuntimeException | Error closedFailure) {
            try {
                factory.close();
            } catch (Exception | Error closeFailure) {
                closedFailure.addSuppressed(closeFailure);
            }
            throw closedFailure;
        }
    }

    /**
     * Builds the source-aware {@link EntryProcessFactory} for {@code format}, mirroring the legacy
     * {@code LakehouseCompactionServiceImpl}: an Ursa source reads straight off the WAL via
     * {@link StorageApi}, while a Kafka source reads via a Kafka consumer.
     */
    EntryProcessFactory buildEntryProcessFactory(EntryFormat format, LakehouseConfiguration lakehouseConfig,
                                                 Properties properties) throws Exception {
        ensureOpen();
        if (format == EntryFormat.URSA) {
            StorageApi storageApi = runtime.storageApi();
            if (storageApi == null) {
                throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                        "Ursa source requires a StorageApi in the MaterializationRuntime to read WAL entries");
            }
            // Metrics are not yet wired into the materialization read path (T10 uses noop throughout).
            return new UrsaEntryProcessFactory(storageApi, CompactionMetrics.NOOP);
        }
        if (format == EntryFormat.KAFKA) {
            return new KafkaEntryProcessFactory(lakehouseConfig);
        }
        throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                "Unsupported materialization entry format: " + format);
    }

    /**
     * Pulls the entry-level event timestamp out of {@link LakehouseEntryMetadata} when present.
     * Raw entries read straight off the WAL carry no metadata yet (the sink decodes the batch and
     * resolves per-message metadata internally), so this falls back to {@code 0L} in that case.
     */
    private static long extractTimestamp(GenericEntry entry) {
        Optional<LakehouseEntryMetadata> metadata = entry.metadata();
        if (metadata.isEmpty()) {
            return 0L;
        }
        LakehouseEntryMetadata m = metadata.get();
        if (m.getEntryHeader() != null) {
            return m.getEntryHeader().writtenTimestamp();
        }
        return 0L;
    }

    @Override
    public void invalidate(StreamIdentifier id) {
        Objects.requireNonNull(id, "id");
        RetainedStream removed;
        synchronized (activeStreamsLifecycleLock) {
            removed = activeStreams.remove(id);
            if (removed != null) {
                removed.markRetired();
            }
        }
        if (removed != null) {
            removed.tryClose();
        }
        retryRetiredStreams();
    }

    @Override
    public void close() {
        List<RetainedStream> streamsToClose;
        synchronized (activeStreamsLifecycleLock) {
            closed = true;
            streamsToClose = new ArrayList<>(activeStreams.values());
            activeStreams.clear();
            streamsToClose.forEach(RetainedStream::markRetired);
        }
        streamsToClose.forEach(RetainedStream::tryClose);
        retryRetiredStreams();
        List<EntryProcessFactory> sourceFactories;
        LakehouseFactory managedFactory;
        synchronized (this) {
            sourceFactories = new ArrayList<>(entryProcessFactories.values());
            entryProcessFactories.clear();
            managedFactory = lakehouseFactory;
            lakehouseFactory = null;
            entryReaderProvider = null;
            factories.clear();
        }
        for (EntryProcessFactory factory : sourceFactories) {
            try {
                factory.close();
            } catch (Exception e) {
                log.warn("Failed to close entry process factory during shutdown", e);
            }
        }
        if (managedFactory != null) {
            try {
                managedFactory.close();
            } catch (Exception e) {
                log.warn("Failed to close lakehouse factory during shutdown", e);
            }
        }
        shutdownRetainedStreamCloseRetryExecutorIfDrained();
    }

    /**
     * Returns the {@link MaterializationRuntime} the service was initialized with.
     * Visible for tests so they can verify the runtime is wired.
     */
    MaterializationRuntime runtime() {
        return runtime;
    }

    /**
     * Returns the {@link MaterializationServiceConfig} the service was initialized with.
     * Visible for tests.
     */
    MaterializationServiceConfig config() {
        return config;
    }

    /**
     * Returns the {@link MaterializationMetrics} the service was initialized with via the runtime.
     * Visible for tests.
     */
    MaterializationMetrics metrics() {
        return runtime == null ? MaterializationMetrics.noop() : runtime.metrics();
    }

    /**
     * Registers a {@link TableMaterializerFactory} directly, bypassing the {@link ServiceLoader}.
     * Visible for tests that want to inject a mocked factory.
     *
     * <p>The orchestrator does not call this; production wiring is exclusively via
     * {@link ServiceLoader} in {@link #initialize(MaterializationRuntime, MaterializationServiceConfig)}.
     */
    synchronized void registerFactory(TableCatalogType type, TableMaterializerFactory factory) {
        ensureOpen();
        factories.put(Objects.requireNonNull(type, "type"), Objects.requireNonNull(factory, "factory"));
    }

    /**
     * Registers the {@link Stream} handle the {@link TableMaterializerFactory} needs when it
     * builds the materializer. The orchestrator is expected to call this once per stream before
     * the first {@link #materialize(MaterializationTask)} invocation.
     */
    @Override
    public void registerActiveStream(StreamIdentifier id, Stream stream) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(stream, "stream");
        RetainedStream previous;
        boolean reject;
        synchronized (activeStreamsLifecycleLock) {
            reject = closed;
            if (reject) {
                previous = null;
            } else {
                RetainedStream current = activeStreams.get(id);
                if (current != null && current.stream() == stream) {
                    return;
                }
                previous = activeStreams.put(id, new RetainedStream(stream));
                if (previous != null) {
                    previous.markRetired();
                }
            }
        }
        if (reject) {
            throw new IllegalStateException("Materialization service is closed");
        }
        if (previous != null) {
            previous.tryClose();
        }
        retryRetiredStreams();
    }

    private ActiveStreamLease acquireActiveStream(StreamIdentifier id) {
        while (true) {
            synchronized (activeStreamsLifecycleLock) {
                if (closed) {
                    return null;
                }
                RetainedStream retained = activeStreams.get(id);
                if (retained == null) {
                    return null;
                }
                ActiveStreamLease lease = retained.acquire();
                if (lease != null) {
                    return lease;
                }
                if (activeStreams.get(id) == retained) {
                    return null;
                }
            }
        }
    }

    private void retryRetiredStreams() {
        for (RetainedStream retained : retiredStreams) {
            retained.tryClose();
        }
    }

    private void shutdownRetainedStreamCloseRetryExecutorIfDrained() {
        if (closed && retiredStreams.isEmpty()) {
            retainedStreamCloseRetryExecutor.shutdownNow();
        }
    }

    private static boolean closeStream(Stream stream) {
        try {
            stream.close();
            return true;
        } catch (Exception | Error e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.warn("Failed to close active stream {}", stream, e);
            return false;
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                    "LakehouseMaterializationService is closed");
        }
    }

    private final class RetainedStream {
        private final Stream stream;
        private int leases;
        private boolean retired;
        private boolean closing;
        private boolean streamClosed;
        private boolean retryScheduled;
        private int closeFailures;

        private RetainedStream(Stream stream) {
            this.stream = stream;
        }

        private Stream stream() {
            return stream;
        }

        private synchronized ActiveStreamLease acquire() {
            if (retired) {
                return null;
            }
            leases++;
            return new ActiveStreamLease(this, stream);
        }

        private void markRetired() {
            synchronized (this) {
                retired = true;
            }
            retiredStreams.add(this);
        }

        private void release() {
            synchronized (this) {
                if (leases <= 0) {
                    throw new IllegalStateException("Active stream lease released more than once");
                }
                leases--;
            }
            tryClose();
        }

        private void tryClose() {
            synchronized (this) {
                if (!retired || leases != 0 || closing || streamClosed) {
                    return;
                }
                closing = true;
            }
            boolean closeSucceeded = closeStream(stream);
            synchronized (this) {
                closing = false;
                if (closeSucceeded) {
                    streamClosed = true;
                } else {
                    closeFailures++;
                }
            }
            if (closeSucceeded) {
                retiredStreams.remove(this);
                shutdownRetainedStreamCloseRetryExecutorIfDrained();
            } else {
                scheduleRetry();
            }
        }

        private void scheduleRetry() {
            long retryDelayMillis;
            synchronized (this) {
                if (!retired || leases != 0 || closing || streamClosed || retryScheduled) {
                    return;
                }
                retryScheduled = true;
                int backoffShift = Math.min(Math.max(closeFailures - 1, 0), 10);
                retryDelayMillis = Math.min(
                        RETAINED_STREAM_CLOSE_RETRY_INITIAL_DELAY_MILLIS << backoffShift,
                        RETAINED_STREAM_CLOSE_RETRY_MAX_DELAY_MILLIS);
            }
            try {
                retainedStreamCloseRetryExecutor.schedule(() -> {
                    synchronized (RetainedStream.this) {
                        retryScheduled = false;
                    }
                    tryClose();
                }, retryDelayMillis, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException rejected) {
                synchronized (this) {
                    retryScheduled = false;
                }
                log.error("Retained stream close retry executor rejected stream {}", stream, rejected);
            }
        }
    }

    private static final class ActiveStreamLease implements AutoCloseable {
        private final RetainedStream retained;
        private final Stream stream;
        private final AtomicBoolean closed = new AtomicBoolean();

        private ActiveStreamLease(RetainedStream retained, Stream stream) {
            this.retained = retained;
            this.stream = stream;
        }

        private Stream stream() {
            return stream;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                retained.release();
            }
        }
    }

    /** Returns true if a {@link TableMaterializerFactory} is registered for the given type. */
    boolean hasFactoryFor(TableCatalogType type) {
        return factories.containsKey(type);
    }

    /**
     * Resolves the {@link TableMaterializationPolicy} from the task. Convenience hook for T10
     * which may need to inspect the effective policy before constructing the materializer.
     */
    static TableMaterializationPolicy effectivePolicy(MaterializationTask task) {
        return task.resolvedMaterialization().effectivePolicy();
    }
}
