/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.Stream;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.materialization.ResolvedMaterialization;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.v2.LakehouseFactory;
import io.lakestream.ursa.materialization.FailureMessageHandler;
import io.lakestream.ursa.materialization.MaterializationException;
import io.lakestream.ursa.materialization.MaterializationMetrics;
import io.lakestream.ursa.materialization.MaterializationRuntime;
import io.lakestream.ursa.materialization.MaterializationServiceConfig;
import io.lakestream.ursa.materialization.MaterializationServiceProvider;
import io.lakestream.ursa.materialization.MaterializationTask;
import io.lakestream.ursa.materialization.TableMaterializer;
import io.lakestream.ursa.materialization.TableMaterializerFactory;
import io.lakestream.ursa.materialization.serde.EntryFormat;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.SchemaEvolutionManager;
import io.lakestream.ursa.materialization.serde.SchemaService;
import io.lakestream.ursa.materialization.serde.TableSchemaService;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.StorageApi;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

@Tag("lakehouse")
class LakehouseMaterializationServiceTest {

    private LakehouseMaterializationService service;
    private MaterializationRuntime runtime;
    private MaterializationServiceConfig config;
    private RecordingMetrics metrics;

    @BeforeEach
    void setUp() {
        metrics = new RecordingMetrics();
        SchemaService<?> schemaService = mock(SchemaService.class);
        SchemaEvolutionManager evolutionManager = mock(SchemaEvolutionManager.class);
        Executor executor = Runnable::run;
        runtime = new MaterializationRuntime(
                schemaService,
                evolutionManager,
                executor,
                LoggerFactory.getLogger(LakehouseMaterializationServiceTest.class),
                metrics,
                FailureMessageHandler.noop());
        config = MaterializationServiceConfig.defaults();
        service = new LakehouseMaterializationService();
    }

    @AfterEach
    void tearDown() {
        if (service != null) {
            service.close();
        }
    }

    @Test
    void buildEntryProcessFactorySelectsUrsaReaderForUrsaSource() throws Exception {
        StorageApi storageApi = mock(StorageApi.class);
        service.initialize(runtime.withStorageApi(storageApi), config);
        EntryProcessFactory factory = service.buildEntryProcessFactory(
                EntryFormat.URSA, new LakehouseConfiguration(new Properties()), new Properties());
        assertThat(factory).isInstanceOf(UrsaEntryProcessFactory.class);
    }

    @Test
    void buildEntryProcessFactoryRequiresStorageApiForUrsaSource() {
        // URSA source with no StorageApi must fail clearly rather than fall through to the Ursa path.
        service.initialize(runtime, config);
        assertThatThrownBy(() -> service.buildEntryProcessFactory(
                EntryFormat.URSA, new LakehouseConfiguration(new Properties()), new Properties()))
                .isInstanceOf(MaterializationException.class)
                .hasMessageContaining("StorageApi");
    }

    @Test
    void buildEntryProcessFactorySelectsKafkaReaderForKafkaSource() throws Exception {
        service.initialize(runtime, config);
        Properties props = new Properties();
        props.setProperty("kafka.consumer.bootstrap.servers", "localhost:9092");
        EntryProcessFactory factory = service.buildEntryProcessFactory(
                EntryFormat.KAFKA, new LakehouseConfiguration(props), props);
        assertThat(factory).isInstanceOf(KafkaEntryProcessFactory.class);
    }

    @Test
    void buildEntryProcessFactoryUsesPassedFormatNotServiceRuntimeFormat() throws Exception {
        // Regression: the reader source is a PER-TASK property (the compaction task's type), not the
        // service-level runtime format. Even when the service runtime was initialized as URSA, a KAFKA
        // task must get the Kafka reader — previously a single cached Ursa factory read the Kafka range
        // off the WAL and failed with NoSuchOffsetException.
        service.initialize(runtime.withEntryFormat(EntryFormat.URSA)
                .withStorageApi(mock(StorageApi.class)), config);
        Properties props = new Properties();
        props.setProperty("kafka.consumer.bootstrap.servers", "localhost:9092");

        EntryProcessFactory factory = service.buildEntryProcessFactory(
                EntryFormat.KAFKA, new LakehouseConfiguration(props), props);

        assertThat(factory).isInstanceOf(KafkaEntryProcessFactory.class);
    }

    @Test
    void initializeAndCloseRoundTrip() {
        // Should not throw with mocked runtime + default config.
        service.initialize(runtime, config);
        assertThat(service.runtime()).isSameAs(runtime);
        assertThat(service.config()).isSameAs(config);
        // Discovery is via ServiceLoader; in unit-test scope, the lakehouse factories are
        // visible from the test classpath, so we expect at least Iceberg + Delta to be present.
        assertThat(service.hasFactoryFor(TableCatalogType.ICEBERG)).isTrue();
        assertThat(service.hasFactoryFor(TableCatalogType.DELTA)).isTrue();

        service.close();
        // close() clears factories so a follow-up materialize() would fail (covered below).
        assertThat(service.hasFactoryFor(TableCatalogType.ICEBERG)).isFalse();
    }

    @Test
    void materializeUnknownCatalogTypeFails() {
        service.initialize(runtime, config);

        // CLICKHOUSE has no ServiceLoader registration in the lakehouse module, so the
        // factory lookup must miss and the service must rethrow as MaterializationException.
        ResolvedMaterialization resolved = new ResolvedMaterialization(
                new TableCatalog("my-ch", TableCatalogType.CLICKHOUSE, Map.of(), Map.of()),
                new TableIdentifier("ns", "tbl"),
                TableMaterializationPolicy.empty());
        MaterializationTask task = new MaterializationTask(
                StreamIdentifier.of("public/default", "t"),
                resolved,
                "default/t-partition-0",
                1L,
                0L,
                0L);

        assertThatThrownBy(() -> service.materialize(task))
                .isInstanceOf(MaterializationException.class)
                .satisfies(t -> {
                    MaterializationException me = (MaterializationException) t;
                    assertThat(me.getExceptionCode()).isEqualTo(ExceptionCode.INTERNAL_ERROR);
                    assertThat(t.getMessage()).contains("CLICKHOUSE");
                });

        assertThat(metrics.evolutionRejections)
                .anySatisfy(entry -> assertThat(entry.reason).contains("CLICKHOUSE"));
    }

    @Test
    void materializeNoneCatalogSkipsFactoryLookup() {
        service.initialize(runtime, config);

        // A NONE catalog marks a managed-only (SBT) task: there is no external factory and none must be
        // required. With no source task the managed writer is not built either, so the service reaches
        // the empty-materializer guard — proving it did NOT take the "no factory registered" path.
        ResolvedMaterialization resolved = new ResolvedMaterialization(
                new TableCatalog("managed-none", TableCatalogType.NONE, Map.of(), Map.of()),
                new TableIdentifier("ns", "tbl"),
                TableMaterializationPolicy.empty());
        MaterializationTask task = new MaterializationTask(
                StreamIdentifier.of("public/default", "t"),
                resolved,
                "default/t-partition-0",
                1L,
                0L,
                0L);

        assertThatThrownBy(() -> service.materialize(task))
                .isInstanceOf(MaterializationException.class)
                .satisfies(t -> {
                    MaterializationException me = (MaterializationException) t;
                    assertThat(me.getExceptionCode()).isEqualTo(ExceptionCode.INTERNAL_ERROR);
                    assertThat(t.getMessage())
                            .contains("No materializer available")
                            .doesNotContain("No TableMaterializerFactory");
                });
    }

    @Test
    void materializeBeforeInitFails() {
        ResolvedMaterialization resolved = new ResolvedMaterialization(
                new TableCatalog("my-ch", TableCatalogType.CLICKHOUSE, Map.of(), Map.of()),
                new TableIdentifier("ns", "tbl"),
                TableMaterializationPolicy.empty());
        MaterializationTask task = new MaterializationTask(
                StreamIdentifier.of("public/default", "t"),
                resolved,
                "default/t-partition-0",
                1L,
                0L,
                0L);

        assertThatThrownBy(() -> service.materialize(task))
                .isInstanceOf(MaterializationException.class)
                .satisfies(t -> {
                    MaterializationException me = (MaterializationException) t;
                    assertThat(me.getExceptionCode()).isEqualTo(ExceptionCode.INTERNAL_ERROR);
                    assertThat(t.getMessage()).contains("not initialized");
                });
    }

    @Test
    void invalidateUnknownStreamIsNoOp() {
        service.initialize(runtime, config);
        // No materializer is registered for this stream; invalidate must silently no-op.
        service.invalidate(StreamIdentifier.of("public/default", "ghost"));
    }

    @Test
    void materializeReadsEntriesFromProviderAndWritesEachThenCommits() throws Exception {
        service.initialize(runtime, config);

        // Inject a stub factory that returns the mock materializer, so materialize()'s factory.create
        // path yields it (no real Delta writer). Materializers are built fresh per task, not cached.
        StreamIdentifier id = StreamIdentifier.of("public/default", "read-loop");
        TableMaterializer<GenericEntry> materializer = mock(TableMaterializer.class);
        registerStubMaterializer(id, materializer);

        // Stub reader yields 3 entries then null; assert 3 writes + 1 commit.
        Deque<GenericEntry> queue = new ArrayDeque<>();
        for (int i = 0; i < 3; i++) {
            queue.add(new GenericEntry(new Entry(EntryHeader.NOT_FOUND, Unpooled.EMPTY_BUFFER)));
        }
        service.setEntryReaderProvider((topic, streamId, start, end) -> new IEntryReader() {
            @Override
            public GenericEntry read() {
                return queue.poll();
            }

            @Override
            public void close() {
            }
        });

        ResolvedMaterialization resolved = new ResolvedMaterialization(
                new TableCatalog("delta-cat", TableCatalogType.DELTA, Map.of(), Map.of()),
                new TableIdentifier("ns", "tbl"),
                TableMaterializationPolicy.empty());
        MaterializationTask task = new MaterializationTask(
                id, resolved, "default/read-loop-partition-0", 1L, 0L, 3L);

        service.materialize(task);

        verify(materializer, times(3)).write(any(), any());
        verify(materializer).commit();
    }

    @Test
    void kafkaMaterializationKeepsCanonicalIdentityButUsesLogicalSourceTopic() throws Exception {
        service.initialize(runtime, config);
        String canonicalTopic = "default/orders-partition-0-topic-id";
        String logicalSourceTopic = "orders-partition-0";
        StreamIdentifier canonicalId = StreamIdentifier.of("default", "orders-topic-id");
        Map<String, String> sourceProperties = Map.of(
                "entryFormat", EntryFormat.KAFKA.name(),
                KafkaEntryProcessFactory.SOURCE_TOPIC_PROPERTY, logicalSourceTopic,
                KafkaEntryProcessFactory.SOURCE_TOPIC_ID_PROPERTY, "iZhG_yJzQmymQLeqSmyE1Q",
                KafkaEntryProcessFactory.SOURCE_SCHEMA_TOPIC_PROPERTY, "orders");
        CompactStreamTask sourceTask = new CompactStreamTask();
        sourceTask.setTopic(canonicalTopic);
        sourceTask.setTaskName("orders-task");
        sourceTask.setStreamId(17L);
        sourceTask.setStartOffset(0L);
        sourceTask.setEndOffset(0L);
        sourceTask.setProperties(sourceProperties);

        AtomicReference<String> readerTopic = new AtomicReference<>();
        service.setEntryReaderProvider((topic, streamId, start, end) -> {
            readerTopic.set(topic);
            return new IEntryReader() {
                @Override
                public GenericEntry read() {
                    return null;
                }

                @Override
                public void close() {
                }
            };
        });

        LakehouseFactory managedFactory = mock(LakehouseFactory.class);
        when(managedFactory.getManagedWriter(eq(canonicalTopic), any())).thenReturn(java.util.Optional.empty());
        service.setLakehouseFactory(managedFactory);

        TableMaterializer<GenericEntry> materializer = mock(TableMaterializer.class);
        Stream activeStream = mock(Stream.class);
        when(activeStream.identifier()).thenReturn(canonicalId);
        service.registerActiveStream(canonicalId, activeStream);
        AtomicReference<StreamIdentifier> materializerStream = new AtomicReference<>();
        AtomicReference<MaterializationRuntime> materializerRuntime = new AtomicReference<>();
        service.registerFactory(TableCatalogType.DELTA, new TableMaterializerFactory() {
            @Override
            public TableCatalogType catalogType() {
                return TableCatalogType.DELTA;
            }

            @Override
            public TableMaterializer<?> create(TableMaterializationPolicy policy, TableCatalog resolvedCatalog,
                                               Stream stream, MaterializationRuntime taskRuntime) {
                materializerStream.set(stream.identifier());
                materializerRuntime.set(taskRuntime);
                return materializer;
            }

            @Override
            public TableSchemaService<?, ?> schemaService(TableMaterializationPolicy policy,
                                                          TableCatalog resolvedCatalog, Stream stream) {
                return null;
            }
        });
        MaterializationTask task = new MaterializationTask(
                canonicalId, deltaResolved(), canonicalTopic, 17L, 0L, 0L, sourceTask);

        service.materialize(task);

        assertThat(readerTopic).hasValue(logicalSourceTopic);
        assertThat(materializerStream).hasValue(canonicalId);
        assertThat(materializerRuntime.get().entryFormat()).isEqualTo(EntryFormat.KAFKA);
        assertThat(materializerRuntime.get().taskProperties()).isEqualTo(sourceProperties);
        verify(managedFactory).getManagedWriter(eq(canonicalTopic), any());
        verify(materializer).commit();
    }

    @Test
    void replacingActiveStreamWaitsForInFlightMaterializationBeforeClosingOldHandle() throws Exception {
        service.initialize(runtime, config);
        StreamIdentifier id = StreamIdentifier.of("public/default", "retained");
        Stream oldStream = mock(Stream.class);
        Stream replacement = mock(Stream.class);
        CountDownLatch oldStreamClosed = new CountDownLatch(1);
        doThrow(new IOException("transient replacement close failure"))
                .doThrow(new IOException("second transient replacement close failure"))
                .doAnswer(invocation -> {
                    oldStreamClosed.countDown();
                    return null;
                })
                .when(oldStream).close();
        service.registerActiveStream(id, oldStream);

        CountDownLatch commitStarted = new CountDownLatch(1);
        CountDownLatch allowCommit = new CountDownLatch(1);
        TableMaterializer<GenericEntry> materializer = mock(TableMaterializer.class);
        when(materializer.commit()).thenAnswer(invocation -> {
            commitStarted.countDown();
            if (!allowCommit.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out waiting to finish materialization");
            }
            return null;
        });
        service.registerFactory(TableCatalogType.DELTA, new TableMaterializerFactory() {
            @Override
            public TableCatalogType catalogType() {
                return TableCatalogType.DELTA;
            }

            @Override
            public TableMaterializer<?> create(TableMaterializationPolicy policy, TableCatalog resolvedCatalog,
                                               Stream stream, MaterializationRuntime materializationRuntime) {
                assertThat(stream).isSameAs(oldStream);
                return materializer;
            }

            @Override
            public TableSchemaService<?, ?> schemaService(TableMaterializationPolicy policy,
                                                          TableCatalog resolvedCatalog, Stream stream) {
                return null;
            }
        });
        service.setEntryReaderProvider((topic, streamId, start, end) -> new IEntryReader() {
            @Override
            public GenericEntry read() {
                return null;
            }

            @Override
            public void close() {
            }
        });
        MaterializationTask task = new MaterializationTask(
                id, deltaResolved(), "public/default/retained-partition-0", 1L, 0L, 0L);

        CompletableFuture<Void> materialization = CompletableFuture.runAsync(() -> service.materialize(task));
        assertThat(commitStarted.await(10, TimeUnit.SECONDS)).isTrue();

        service.registerActiveStream(id, replacement);
        verify(oldStream, never()).close();

        allowCommit.countDown();
        materialization.get(10, TimeUnit.SECONDS);
        assertThat(oldStreamClosed.await(10, TimeUnit.SECONDS)).isTrue();
        verify(oldStream, times(3)).close();

        service.close();
        verify(replacement).close();
    }

    @Test
    void invalidateAndCloseReleaseRetainedStreamHandles() throws Exception {
        service.initialize(runtime, config);
        StreamIdentifier invalidatedId = StreamIdentifier.of("public/default", "invalidated");
        StreamIdentifier shutdownId = StreamIdentifier.of("public/default", "shutdown");
        Stream invalidated = mock(Stream.class);
        Stream shutdown = mock(Stream.class);
        service.registerActiveStream(invalidatedId, invalidated);
        service.registerActiveStream(shutdownId, shutdown);

        service.invalidate(invalidatedId);
        verify(invalidated).close();
        verify(shutdown, never()).close();

        service.close();
        verify(shutdown).close();
    }

    @Test
    void shutdownRetriesTransientStreamCloseFailure() throws Exception {
        service.initialize(runtime, config);
        StreamIdentifier id = StreamIdentifier.of("public/default", "transient-close");
        Stream stream = mock(Stream.class);
        doThrow(new IOException("transient close failure"))
                .doNothing()
                .when(stream).close();
        service.registerActiveStream(id, stream);

        service.close();

        // retire() makes the first attempt and the shutdown retry pass closes the retained handle.
        verify(stream, times(2)).close();
    }

    @Test
    void shutdownRetriesTransientCloseAfterLastInFlightLeaseIsReleased() throws Exception {
        service.initialize(runtime, config);
        StreamIdentifier id = StreamIdentifier.of("public/default", "shutdown-in-flight");
        Stream stream = mock(Stream.class);
        CountDownLatch streamClosed = new CountDownLatch(1);
        doThrow(new IOException("transient release close failure"))
                .doThrow(new IOException("second transient release close failure"))
                .doAnswer(invocation -> {
                    streamClosed.countDown();
                    return null;
                })
                .when(stream).close();
        service.registerActiveStream(id, stream);

        CountDownLatch commitStarted = new CountDownLatch(1);
        CountDownLatch allowCommit = new CountDownLatch(1);
        TableMaterializer<GenericEntry> materializer = mock(TableMaterializer.class);
        when(materializer.commit()).thenAnswer(invocation -> {
            commitStarted.countDown();
            if (!allowCommit.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out waiting to finish materialization");
            }
            return null;
        });
        service.registerFactory(TableCatalogType.DELTA, new TableMaterializerFactory() {
            @Override
            public TableCatalogType catalogType() {
                return TableCatalogType.DELTA;
            }

            @Override
            public TableMaterializer<?> create(TableMaterializationPolicy policy,
                                               TableCatalog resolvedCatalog,
                                               Stream activeStream,
                                               MaterializationRuntime materializationRuntime) {
                assertThat(activeStream).isSameAs(stream);
                return materializer;
            }

            @Override
            public TableSchemaService<?, ?> schemaService(TableMaterializationPolicy policy,
                                                          TableCatalog resolvedCatalog,
                                                          Stream activeStream) {
                return null;
            }
        });
        service.setEntryReaderProvider((topic, streamId, start, end) -> new IEntryReader() {
            @Override
            public GenericEntry read() {
                return null;
            }

            @Override
            public void close() {
            }
        });
        MaterializationTask task = new MaterializationTask(
                id, deltaResolved(), "public/default/shutdown-in-flight-partition-0", 1L, 0L, 0L);

        CompletableFuture<Void> materialization = CompletableFuture.runAsync(() -> service.materialize(task));
        assertThat(commitStarted.await(10, TimeUnit.SECONDS)).isTrue();

        service.close();
        verify(stream, never()).close();

        allowCommit.countDown();
        materialization.get(10, TimeUnit.SECONDS);
        assertThat(streamClosed.await(10, TimeUnit.SECONDS)).isTrue();
        verify(stream, times(3)).close();
    }

    @Test
    void concurrentReplacementAndShutdownCannotLoseCloseRetry() throws Exception {
        service.initialize(runtime, config);
        StreamIdentifier id = StreamIdentifier.of("public/default", "replacement-shutdown-race");
        Stream oldStream = mock(Stream.class);
        Stream replacement = mock(Stream.class);
        CountDownLatch firstCloseStarted = new CountDownLatch(1);
        CountDownLatch allowFirstClose = new CountDownLatch(1);
        CountDownLatch oldStreamClosed = new CountDownLatch(1);
        AtomicInteger closeAttempts = new AtomicInteger();
        doAnswer(invocation -> {
            int attempt = closeAttempts.incrementAndGet();
            if (attempt == 1) {
                firstCloseStarted.countDown();
                if (!allowFirstClose.await(10, TimeUnit.SECONDS)) {
                    throw new AssertionError("Timed out waiting to fail the first close");
                }
            }
            if (attempt <= 2) {
                throw new IOException("transient close failure " + attempt);
            }
            oldStreamClosed.countDown();
            return null;
        }).when(oldStream).close();
        service.registerActiveStream(id, oldStream);

        CompletableFuture<Void> replacing = CompletableFuture.runAsync(
                () -> service.registerActiveStream(id, replacement));
        assertThat(firstCloseStarted.await(10, TimeUnit.SECONDS)).isTrue();

        service.close();
        verify(replacement).close();
        allowFirstClose.countDown();
        replacing.get(10, TimeUnit.SECONDS);

        assertThat(oldStreamClosed.await(10, TimeUnit.SECONDS)).isTrue();
        verify(oldStream, times(3)).close();
    }

    @Test
    void concurrentInvalidationAndShutdownCannotLoseCloseRetry() throws Exception {
        service.initialize(runtime, config);
        StreamIdentifier id = StreamIdentifier.of("public/default", "invalidation-shutdown-race");
        Stream stream = mock(Stream.class);
        CountDownLatch firstCloseStarted = new CountDownLatch(1);
        CountDownLatch allowFirstClose = new CountDownLatch(1);
        CountDownLatch streamClosed = new CountDownLatch(1);
        AtomicInteger closeAttempts = new AtomicInteger();
        doAnswer(invocation -> {
            int attempt = closeAttempts.incrementAndGet();
            if (attempt == 1) {
                firstCloseStarted.countDown();
                if (!allowFirstClose.await(10, TimeUnit.SECONDS)) {
                    throw new AssertionError("Timed out waiting to fail the first close");
                }
            }
            if (attempt <= 2) {
                throw new IOException("transient close failure " + attempt);
            }
            streamClosed.countDown();
            return null;
        }).when(stream).close();
        service.registerActiveStream(id, stream);

        CompletableFuture<Void> invalidating = CompletableFuture.runAsync(() -> service.invalidate(id));
        assertThat(firstCloseStarted.await(10, TimeUnit.SECONDS)).isTrue();

        service.close();
        allowFirstClose.countDown();
        invalidating.get(10, TimeUnit.SECONDS);

        assertThat(streamClosed.await(10, TimeUnit.SECONDS)).isTrue();
        verify(stream, times(3)).close();
    }

    @Test
    void closedRegistrationRejectsWithoutTakingStreamOwnership() throws Exception {
        service.initialize(runtime, config);
        service.close();
        Stream stream = mock(Stream.class);

        assertThatThrownBy(() -> service.registerActiveStream(
                StreamIdentifier.of("public/default", "late"), stream))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");

        verify(stream, never()).close();
    }

    @Test
    void shutdownFencesMaterializerThatSurvivesFactoryCreation() throws Exception {
        service.initialize(runtime, config);
        StreamIdentifier id = StreamIdentifier.of("public/default", "blocked-factory");
        Stream stream = mock(Stream.class);
        service.registerActiveStream(id, stream);

        CountDownLatch factoryEntered = new CountDownLatch(1);
        CountDownLatch allowFactoryToReturn = new CountDownLatch(1);
        TableMaterializer<GenericEntry> materializer = mock(TableMaterializer.class);
        service.registerFactory(TableCatalogType.DELTA, new TableMaterializerFactory() {
            @Override
            public TableCatalogType catalogType() {
                return TableCatalogType.DELTA;
            }

            @Override
            public TableMaterializer<?> create(TableMaterializationPolicy policy, TableCatalog resolvedCatalog,
                                               Stream activeStream, MaterializationRuntime taskRuntime) {
                factoryEntered.countDown();
                try {
                    if (!allowFactoryToReturn.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("Timed out waiting to release materializer factory");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("Interrupted while waiting to release materializer factory", e);
                }
                return materializer;
            }

            @Override
            public TableSchemaService<?, ?> schemaService(TableMaterializationPolicy policy,
                                                          TableCatalog resolvedCatalog, Stream activeStream) {
                return null;
            }
        });
        AtomicBoolean readerOpened = new AtomicBoolean();
        service.setEntryReaderProvider((topic, streamId, start, end) -> {
            readerOpened.set(true);
            throw new AssertionError("reader must not open after shutdown");
        });
        MaterializationTask task = new MaterializationTask(
                id, deltaResolved(), "public/default/blocked-factory-partition-0", 1L, 0L, 0L);

        CompletableFuture<Void> materialization = CompletableFuture.runAsync(() -> service.materialize(task));
        assertThat(factoryEntered.await(10, TimeUnit.SECONDS)).isTrue();

        service.close();
        verify(stream, never()).close();
        allowFactoryToReturn.countDown();

        assertThatThrownBy(() -> materialization.get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(MaterializationException.class)
                .hasRootCauseMessage("LakehouseMaterializationService is closed");
        assertThat(readerOpened).isFalse();
        verify(materializer).close();
        verify(stream).close();
    }

    @Test
    void shutdownClosesSourceFactoryBuiltAfterClosedFence() throws Exception {
        CountDownLatch factoryBuildStarted = new CountDownLatch(1);
        CountDownLatch allowFactoryToReturn = new CountDownLatch(1);
        EntryProcessFactory sourceFactory = mock(EntryProcessFactory.class);
        LakehouseMaterializationService blockingService = new LakehouseMaterializationService() {
            @Override
            EntryProcessFactory buildEntryProcessFactory(EntryFormat format,
                                                         LakehouseConfiguration lakehouseConfig,
                                                         Properties properties) {
                factoryBuildStarted.countDown();
                try {
                    if (!allowFactoryToReturn.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("Timed out waiting to release source factory creation");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("Interrupted while waiting to release source factory creation", e);
                }
                return sourceFactory;
            }
        };
        blockingService.initialize(runtime, config);
        CountDownLatch shutdownStarted = new CountDownLatch(1);
        Stream retainedStream = mock(Stream.class);
        doAnswer(invocation -> {
            shutdownStarted.countDown();
            return null;
        }).when(retainedStream).close();
        blockingService.registerActiveStream(
                StreamIdentifier.of("public/default", "factory-close-race"), retainedStream);
        CompletableFuture<EntryProcessFactory> building = CompletableFuture.supplyAsync(() -> {
            try {
                return blockingService.entryProcessFactoryFor(EntryFormat.KAFKA, new Properties());
            } catch (RuntimeException | Error e) {
                throw e;
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        assertThat(factoryBuildStarted.await(10, TimeUnit.SECONDS)).isTrue();

        CompletableFuture<Void> closing = CompletableFuture.runAsync(blockingService::close);
        assertThat(shutdownStarted.await(10, TimeUnit.SECONDS)).isTrue();
        allowFactoryToReturn.countDown();

        assertThatThrownBy(() -> building.get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(MaterializationException.class)
                .hasRootCauseMessage("LakehouseMaterializationService is closed");
        closing.get(10, TimeUnit.SECONDS);
        verify(sourceFactory).close();
        verify(retainedStream).close();
    }

    @Test
    void materializeCommitsSingleUseMaterializer() {
        // A TableMaterializer is single-use (commit() is terminal); the service builds a fresh one per
        // task via the factory (never caches), so it commits the task's materializer exactly once.
        service.initialize(runtime, config);
        StreamIdentifier id = StreamIdentifier.of("public/default", "commit");
        TableMaterializer<GenericEntry> materializer = mock(TableMaterializer.class);
        registerStubMaterializer(id, materializer);
        service.setEntryReaderProvider((topic, streamId, start, end) -> new IEntryReader() {
            @Override
            public GenericEntry read() {
                return null; // no entries: exercise the write-loop + commit path only
            }

            @Override
            public void close() {
            }
        });
        MaterializationTask task = new MaterializationTask(id, deltaResolved(),
                "default/commit-partition-0", 1L, 0L, 0L);

        service.materialize(task);

        verify(materializer).commit();
    }

    @Test
    void materializeClosesMaterializerOnFailure() {
        service.initialize(runtime, config);
        StreamIdentifier id = StreamIdentifier.of("public/default", "fail");
        TableMaterializer<GenericEntry> materializer = mock(TableMaterializer.class);
        doThrow(new MaterializationException(ExceptionCode.INTERNAL_ERROR, "boom"))
                .when(materializer).write(any(), any());
        registerStubMaterializer(id, materializer);
        Deque<GenericEntry> queue = new ArrayDeque<>();
        queue.add(new GenericEntry(new Entry(EntryHeader.NOT_FOUND, Unpooled.EMPTY_BUFFER)));
        service.setEntryReaderProvider((topic, streamId, start, end) -> new IEntryReader() {
            @Override
            public GenericEntry read() {
                return queue.poll();
            }

            @Override
            public void close() {
            }
        });
        MaterializationTask task = new MaterializationTask(id, deltaResolved(),
                "default/fail-partition-0", 1L, 0L, 1L);

        assertThatThrownBy(() -> service.materialize(task)).isInstanceOf(MaterializationException.class);

        // Failure path: the (uncommitted) writer is closed for resource cleanup.
        verify(materializer).close();
    }

    @Test
    void materializerFailureConsumesDuplicateWhileServiceReleasesSourceEntry() {
        service.initialize(runtime, config);
        StreamIdentifier id = StreamIdentifier.of("public/default", "owned-failure");
        TableMaterializer<GenericEntry> materializer = mock(TableMaterializer.class);
        doAnswer(invocation -> {
            GenericEntry transferred = invocation.getArgument(0);
            transferred.entry().payload().release();
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR, "boom");
        }).when(materializer).write(any(), any());
        registerStubMaterializer(id, materializer);

        var sourcePayload = Unpooled.buffer().writeByte(1);
        Deque<GenericEntry> queue = new ArrayDeque<>();
        queue.add(new GenericEntry(new Entry(EntryHeader.NOT_FOUND, sourcePayload)));
        service.setEntryReaderProvider((topic, streamId, start, end) -> new IEntryReader() {
            @Override
            public GenericEntry read() {
                return queue.poll();
            }

            @Override
            public void close() {
            }
        });
        MaterializationTask task = new MaterializationTask(id, deltaResolved(),
                "default/owned-failure-partition-0", 1L, 0L, 1L);

        assertThatThrownBy(() -> service.materialize(task))
                .isInstanceOf(MaterializationException.class)
                .hasMessageContaining("boom");

        assertThat(sourcePayload.refCnt()).isZero();
        verify(materializer).close();
    }

    @Test
    void shutdownAfterReaderReturnsEntryStillReleasesOwnedPayload() {
        service.initialize(runtime, config);
        StreamIdentifier id = StreamIdentifier.of("public/default", "shutdown-after-read");
        TableMaterializer<GenericEntry> materializer = mock(TableMaterializer.class);
        registerStubMaterializer(id, materializer);
        var sourcePayload = Unpooled.buffer().writeByte(1);
        AtomicBoolean returned = new AtomicBoolean();
        service.setEntryReaderProvider((topic, streamId, start, end) -> new IEntryReader() {
            @Override
            public GenericEntry read() {
                if (!returned.compareAndSet(false, true)) {
                    return null;
                }
                service.close();
                return new GenericEntry(new Entry(EntryHeader.NOT_FOUND, sourcePayload));
            }

            @Override
            public void close() {
            }
        });
        MaterializationTask task = new MaterializationTask(
                id, deltaResolved(), "default/shutdown-after-read-partition-0", 1L, 0L, 1L);

        assertThatThrownBy(() -> service.materialize(task))
                .isInstanceOf(MaterializationException.class)
                .hasMessageContaining("closed");

        assertThat(sourcePayload.refCnt()).isZero();
        verify(materializer, never()).write(any(), any());
        verify(materializer).close();
    }

    private static ResolvedMaterialization deltaResolved() {
        return new ResolvedMaterialization(
                new TableCatalog("delta-cat", TableCatalogType.DELTA, Map.of(), Map.of()),
                new TableIdentifier("ns", "tbl"),
                TableMaterializationPolicy.empty());
    }

    /**
     * Registers the active Stream handle plus a stub {@link TableMaterializerFactory} (for the DELTA
     * catalog type used by these tests) that returns {@code materializer}, so materialize()'s
     * factory.create path yields the test double. Materializers are built fresh per task, never cached.
     */
    private void registerStubMaterializer(StreamIdentifier id, TableMaterializer<?> materializer) {
        service.registerActiveStream(id, mock(Stream.class));
        service.registerFactory(TableCatalogType.DELTA, new TableMaterializerFactory() {
            @Override
            public TableCatalogType catalogType() {
                return TableCatalogType.DELTA;
            }

            @Override
            public TableMaterializer<?> create(TableMaterializationPolicy policy, TableCatalog resolvedCatalog,
                                               Stream stream, MaterializationRuntime materializationRuntime) {
                return materializer;
            }

            @Override
            public TableSchemaService<?, ?> schemaService(TableMaterializationPolicy policy,
                                                          TableCatalog resolvedCatalog, Stream stream) {
                return null;
            }
        });
    }

    @Test
    void materializationServiceProviderLoadsDefaultClassName() {
        // The default deployment loads LakehouseMaterializationService by class name; the
        // provider must instantiate it via the public no-arg constructor.
        var instance = MaterializationServiceProvider
                .load(LakehouseMaterializationService.class.getName());
        assertThat(instance).isInstanceOf(LakehouseMaterializationService.class);
        instance.close();
    }

    @Test
    void materializationServiceProviderRejectsMissingClass() {
        assertThatThrownBy(() ->
                MaterializationServiceProvider.load("io.lakestream.does.not.Exist"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to load MaterializationService class");
    }

    /**
     * Records metric invocations so tests can assert on the catalog/type/reason without
     * pulling in a heavy Micrometer test harness. The lakehouse module has no preferred
     * test metric registry today.
     */
    private static final class RecordingMetrics implements MaterializationMetrics {
        record EvolutionRejection(String catalog, TableCatalogType type,
                                   StreamIdentifier stream, String reason) {
        }

        final java.util.List<EvolutionRejection> evolutionRejections = new java.util.ArrayList<>();

        @Override
        public void recordWritten(String catalog, TableCatalogType catalogType,
                                  StreamIdentifier stream) {
            // ignored for these tests
        }

        @Override
        public void recordCommitDuration(String catalog, TableCatalogType catalogType,
                                         StreamIdentifier stream, long nanos) {
        }

        @Override
        public void recordCommitRetry(String catalog, TableCatalogType catalogType,
                                      StreamIdentifier stream, boolean success) {
        }

        @Override
        public void recordSchemaEvolutionApplied(String catalog, TableCatalogType catalogType,
                                                 StreamIdentifier stream, String operation) {
        }

        @Override
        public void recordSchemaEvolutionRejected(String catalog, TableCatalogType catalogType,
                                                  StreamIdentifier stream, String reason) {
            evolutionRejections.add(new EvolutionRejection(catalog, catalogType, stream, reason));
        }

        @Override
        public void recordDlqRecord(String catalog, TableCatalogType catalogType,
                                    StreamIdentifier stream) {
        }

        @Override
        public void setState(String catalog, TableCatalogType catalogType,
                             StreamIdentifier stream,
                             io.lakestream.api.materialization.MaterializationState state) {
        }
    }
}
