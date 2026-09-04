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
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.StreamMetadata;
import io.lakestream.api.materialization.MaterializationState;
import io.lakestream.api.materialization.ResolvedMaterialization;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableConf;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.api.materialization.TableMode;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.utils.StreamTableNaming;
import io.lakestream.ursa.lakehouse.v2.AbstractLakehouseWriter;
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
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.SchemaEvolutionManager;
import io.lakestream.ursa.materialization.serde.SchemaService;
import io.lakestream.ursa.materialization.serde.TableSchemaService;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSourceMetadata;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.StorageApi;
import io.netty.buffer.Unpooled;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    void buildEntryProcessFactoryUsesStorageApi() throws Exception {
        StorageApi storageApi = mock(StorageApi.class);
        service.initialize(runtime.withStorageApi(storageApi), config);
        EntryProcessFactory factory = service.buildEntryProcessFactory();
        assertThat(factory).isInstanceOf(UrsaEntryProcessFactory.class);
    }

    @Test
    void buildEntryProcessFactoryRequiresStorageApi() {
        service.initialize(runtime, config);
        assertThatThrownBy(service::buildEntryProcessFactory)
                .isInstanceOf(MaterializationException.class)
                .hasMessageContaining("StorageApi");
    }

    @Test
    void initializeAndCloseRoundTrip() {
        service.initialize(runtime, config);
        assertThat(service.runtime()).isSameAs(runtime);
        assertThat(service.config()).isSameAs(config);
        assertThat(service.hasFactoryFor(TableCatalogType.ICEBERG)).isTrue();
        assertThat(service.hasFactoryFor(TableCatalogType.DELTA)).isTrue();

        service.close();

        assertThat(service.hasFactoryFor(TableCatalogType.ICEBERG)).isFalse();
    }

    @Test
    void materializeUnknownCatalogTypeFails() {
        service.initialize(runtime, config);
        ResolvedMaterialization resolved = new ResolvedMaterialization(
                new TableCatalog("my-ch", TableCatalogType.CLICKHOUSE, Map.of(), Map.of()),
                new TableIdentifier("ns", "tbl"),
                TableMaterializationPolicy.empty());

        assertThatThrownBy(() -> service.materialize(task(
                metadata("public/default", "t", Map.of()), resolved, "default/t-partition-0", 0L)))
                .isInstanceOf(MaterializationException.class)
                .hasMessageContaining("CLICKHOUSE");
        assertThat(metrics.evolutionRejections)
                .anySatisfy(entry -> assertThat(entry.reason).contains("CLICKHOUSE"));
    }

    @Test
    void materializeNoneCatalogSkipsFactoryLookup() {
        service.initialize(runtime, config);
        ResolvedMaterialization resolved = new ResolvedMaterialization(
                new TableCatalog("managed-none", TableCatalogType.NONE, Map.of(), Map.of()),
                new TableIdentifier("ns", "tbl"),
                TableMaterializationPolicy.empty());

        assertThatThrownBy(() -> service.materialize(task(
                metadata("public/default", "t", Map.of()), resolved, "default/t-partition-0", 0L)))
                .isInstanceOf(MaterializationException.class)
                .hasMessageContaining("No materializer available")
                .satisfies(error -> assertThat(error.getMessage())
                        .doesNotContain("No TableMaterializerFactory"));
    }

    @Test
    void managedOnlyMaterializationUsesLogicalTopicFromMetadata() throws Exception {
        service.initialize(runtime, config);
        String sourceTopic = "default/orders-topic-id-partition-0";
        Map<String, String> effectiveProperties = Map.of(
                "sbtEnabled", "true",
                KafkaSourceMetadata.TOPIC_NAME_PROPERTY, "orders");
        CompactStreamTask sourceTask = sourceTask(
                sourceTopic, 17L, Map.of("sbtEnabled", "true"));
        service.setEntryReaderProvider(emptyReader());
        AbstractLakehouseWriter managedWriter = mock(AbstractLakehouseWriter.class);
        LakehouseFactory managedFactory = mock(LakehouseFactory.class);
        when(managedFactory.getManagedWriter(sourceTopic, effectiveProperties))
                .thenReturn(java.util.Optional.of(managedWriter));
        service.setLakehouseFactory(managedFactory);
        ResolvedMaterialization resolved = new ResolvedMaterialization(
                new TableCatalog("managed-none", TableCatalogType.NONE, Map.of(), Map.of()),
                new TableIdentifier("ns", "tbl"),
                TableMaterializationPolicy.empty());
        StreamMetadata metadata = metadata(
                "default", "orders-topic-id",
                Map.of(KafkaSourceMetadata.TOPIC_NAME_PROPERTY, "orders"));

        service.materialize(new MaterializationTask(
                metadata, resolved, sourceTopic, 17L, 0L, 0L, sourceTask));

        verify(managedFactory).getManagedWriter(sourceTopic, effectiveProperties);
        verify(managedWriter).close();
    }

    @Test
    void managedCatalogMaterializerDoesNotAlsoBuildStandaloneSbtWriter() {
        service.initialize(runtime, config);
        String sourceTopic = "default/orders-topic-id-abc-partition-0";
        CompactStreamTask sourceTask = sourceTask(sourceTopic, 17L, Map.of("sbtEnabled", "true"));
        service.setEntryReaderProvider(emptyReader());
        LakehouseFactory managedFactory = mock(LakehouseFactory.class);
        service.setLakehouseFactory(managedFactory);
        TableMaterializer<GenericEntry> catalogMaterializer = mock(TableMaterializer.class);
        service.registerFactory(TableCatalogType.ICEBERG, new TableMaterializerFactory() {
            @Override
            public TableCatalogType catalogType() {
                return TableCatalogType.ICEBERG;
            }

            @Override
            public TableMaterializer<?> create(
                    TableMaterializationPolicy policy,
                    TableCatalog resolvedCatalog,
                    StreamMetadata streamMetadata,
                    MaterializationRuntime materializationRuntime) {
                return catalogMaterializer;
            }

            @Override
            public TableSchemaService<?, ?> schemaService(
                    TableMaterializationPolicy policy,
                    TableCatalog resolvedCatalog,
                    StreamMetadata streamMetadata) {
                return null;
            }
        });
        TableIdentifier identifier = new TableIdentifier("default", "orders-topic-id-abc");
        TableMaterializationPolicy policy = policyWithMode(
                "managed-iceberg", identifier, TableMode.MANAGED);
        ResolvedMaterialization resolved = new ResolvedMaterialization(
                new TableCatalog("managed-iceberg", TableCatalogType.ICEBERG, Map.of(), Map.of()),
                identifier,
                policy);

        service.materialize(new MaterializationTask(
                metadata("default", "orders-topic-id-abc", Map.of()),
                resolved, sourceTopic, 17L, 0L, 0L, sourceTask));

        verify(catalogMaterializer).commit();
        verify(managedFactory, never()).getManagedWriter(any(), any());
        assertThat(sourceTask.getProperties())
                .containsEntry(StreamTableNaming.RESOLVED_TABLE_NAMESPACE_PROPERTY, "default")
                .containsEntry(StreamTableNaming.RESOLVED_TABLE_NAME_PROPERTY, "orders-topic-id-abc")
                .containsEntry(LakehouseConfiguration.STREAM_TABLE_MODE, TableMode.MANAGED.name())
                .containsEntry(LakehouseConfiguration.CATALOG_NAME, "managed-iceberg")
                .containsEntry("lakehouseType", LakehouseConfiguration.LakehouseType.ICEBERG.name());
    }

    @Test
    void materializeBeforeInitFails() {
        assertThatThrownBy(() -> service.materialize(task(
                metadata("public/default", "t", Map.of()),
                deltaResolved(), "default/t-partition-0", 0L)))
                .isInstanceOf(MaterializationException.class)
                .hasMessageContaining("not initialized");
    }

    @Test
    void invalidateIsNoOpWithoutRetainedHandles() {
        service.initialize(runtime, config);
        service.invalidate(StreamIdentifier.of("public/default", "ghost"));
    }

    @Test
    void materializeReadsEntriesFromProviderAndWritesEachThenCommits() throws Exception {
        service.initialize(runtime, config);
        StreamMetadata metadata = metadata("public/default", "read-loop", Map.of());
        TableMaterializer<GenericEntry> materializer = mock(TableMaterializer.class);
        registerStubMaterializer(materializer);
        Deque<GenericEntry> queue = new ArrayDeque<>();
        for (int i = 0; i < 3; i++) {
            queue.add(new GenericEntry(new Entry(EntryHeader.NOT_FOUND, Unpooled.EMPTY_BUFFER)));
        }
        service.setEntryReaderProvider((topic, streamId, start, end) -> reader(queue));

        service.materialize(task(
                metadata, deltaResolved(), "default/read-loop-partition-0", 3L));

        verify(materializer, times(3)).write(any(), any());
        verify(materializer).commit();
    }

    @Test
    void materializationUsesCanonicalStorageTopicAndMetadataProperties() throws Exception {
        service.initialize(runtime, config);
        String canonicalTopic = "default/orders-partition-0-topic-id";
        StreamIdentifier canonicalId = StreamIdentifier.of("default", "orders-topic-id");
        CompactStreamTask sourceTask = sourceTask(canonicalTopic, 17L, Map.of(
                "sdtCatalogName", "orders-catalog",
                KafkaSourceMetadata.LOGICAL_NAME_PROPERTY, "stale-logical-name",
                KafkaSourceMetadata.TOPIC_NAME_PROPERTY, "stale-topic-name"));
        AtomicReference<String> readerTopic = new AtomicReference<>();
        service.setEntryReaderProvider((topic, streamId, start, end) -> {
            readerTopic.set(topic);
            return reader(new ArrayDeque<>());
        });
        LakehouseFactory managedFactory = mock(LakehouseFactory.class);
        when(managedFactory.getManagedWriter(eq(canonicalTopic), any()))
                .thenReturn(java.util.Optional.empty());
        service.setLakehouseFactory(managedFactory);
        TableMaterializer<GenericEntry> materializer = mock(TableMaterializer.class);
        AtomicReference<TableMaterializationPolicy> factoryPolicy = new AtomicReference<>();
        AtomicReference<StreamMetadata> factoryMetadata = new AtomicReference<>();
        AtomicReference<MaterializationRuntime> factoryRuntime = new AtomicReference<>();
        service.registerFactory(TableCatalogType.DELTA, new TableMaterializerFactory() {
            @Override
            public TableCatalogType catalogType() {
                return TableCatalogType.DELTA;
            }

            @Override
            public TableMaterializer<?> create(
                    TableMaterializationPolicy policy,
                    TableCatalog resolvedCatalog,
                    StreamMetadata streamMetadata,
                    MaterializationRuntime taskRuntime) {
                factoryPolicy.set(policy);
                factoryMetadata.set(streamMetadata);
                factoryRuntime.set(taskRuntime);
                return materializer;
            }

            @Override
            public TableSchemaService<?, ?> schemaService(
                    TableMaterializationPolicy policy,
                    TableCatalog resolvedCatalog,
                    StreamMetadata streamMetadata) {
                return null;
            }
        });
        StreamMetadata metadata = metadata(canonicalId, Map.of(
                KafkaSourceMetadata.LOGICAL_NAME_PROPERTY, "orders",
                KafkaSourceMetadata.TOPIC_NAME_PROPERTY, "orders"));

        service.materialize(new MaterializationTask(
                metadata, deltaResolved(), canonicalTopic, 17L, 0L, 0L, sourceTask));

        assertThat(readerTopic).hasValue(canonicalTopic);
        assertThat(factoryPolicy.get().tableIdentifier())
                .contains(new TableIdentifier("ns", "tbl"));
        assertThat(factoryMetadata).hasValue(metadata);
        assertThat(factoryRuntime.get().taskProperties()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "sdtCatalogName", "orders-catalog",
                KafkaSourceMetadata.LOGICAL_NAME_PROPERTY, "orders",
                KafkaSourceMetadata.TOPIC_NAME_PROPERTY, "orders",
                MaterializationRuntime.SOURCE_TOPIC_PROPERTY, canonicalTopic));
        verify(managedFactory).getManagedWriter(canonicalTopic, Map.of(
                "sdtCatalogName", "orders-catalog",
                KafkaSourceMetadata.LOGICAL_NAME_PROPERTY, "orders",
                KafkaSourceMetadata.TOPIC_NAME_PROPERTY, "orders"));
        assertThat(sourceTask.getProperties())
                .containsEntry(StreamTableNaming.RESOLVED_TABLE_NAMESPACE_PROPERTY, "ns")
                .containsEntry(StreamTableNaming.RESOLVED_TABLE_NAME_PROPERTY, "tbl")
                .containsEntry(LakehouseConfiguration.STREAM_TABLE_MODE, TableMode.EXTERNAL.name())
                .containsEntry(LakehouseConfiguration.CATALOG_NAME, "delta-cat")
                .containsEntry("lakehouseType", LakehouseConfiguration.LakehouseType.DELTA.name());
        verify(materializer).commit();
    }

    @Test
    void shutdownFencesMaterializerThatSurvivesFactoryCreation() throws Exception {
        service.initialize(runtime, config);
        StreamMetadata metadata = metadata("public/default", "blocked-factory", Map.of());
        CountDownLatch factoryEntered = new CountDownLatch(1);
        CountDownLatch allowFactoryToReturn = new CountDownLatch(1);
        TableMaterializer<GenericEntry> materializer = mock(TableMaterializer.class);
        service.registerFactory(TableCatalogType.DELTA, blockingFactory(
                factoryEntered, allowFactoryToReturn, materializer));
        AtomicBoolean readerOpened = new AtomicBoolean();
        service.setEntryReaderProvider((topic, streamId, start, end) -> {
            readerOpened.set(true);
            throw new AssertionError("reader must not open after shutdown");
        });
        MaterializationTask task = task(
                metadata, deltaResolved(), "public/default/blocked-factory-partition-0", 0L);

        CompletableFuture<Void> materialization =
                CompletableFuture.runAsync(() -> service.materialize(task));
        assertThat(factoryEntered.await(10, TimeUnit.SECONDS)).isTrue();
        service.close();
        allowFactoryToReturn.countDown();

        assertThatThrownBy(() -> materialization.get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(MaterializationException.class)
                .hasRootCauseMessage("LakehouseMaterializationService is closed");
        assertThat(readerOpened).isFalse();
        verify(materializer).close();
    }

    @Test
    void closeClosesBuiltSourceFactory() throws Exception {
        EntryProcessFactory sourceFactory = mock(EntryProcessFactory.class);
        service = new LakehouseMaterializationService() {
            @Override
            EntryProcessFactory buildEntryProcessFactory() {
                return sourceFactory;
            }
        };
        service.initialize(runtime, config);
        assertThat(service.entryProcessFactory()).isSameAs(sourceFactory);

        service.close();

        verify(sourceFactory).close();
    }

    @Test
    void materializeCommitsSingleUseMaterializer() {
        service.initialize(runtime, config);
        StreamMetadata metadata = metadata("public/default", "commit", Map.of());
        TableMaterializer<GenericEntry> materializer = mock(TableMaterializer.class);
        registerStubMaterializer(materializer);
        service.setEntryReaderProvider(emptyReader());

        service.materialize(task(
                metadata, deltaResolved(), "default/commit-partition-0", 0L));

        verify(materializer).commit();
    }

    @Test
    void materializeClosesMaterializerOnFailure() {
        service.initialize(runtime, config);
        StreamMetadata metadata = metadata("public/default", "fail", Map.of());
        TableMaterializer<GenericEntry> materializer = mock(TableMaterializer.class);
        doThrow(new MaterializationException(ExceptionCode.INTERNAL_ERROR, "boom"))
                .when(materializer).write(any(), any());
        registerStubMaterializer(materializer);
        Deque<GenericEntry> queue = new ArrayDeque<>();
        queue.add(new GenericEntry(new Entry(EntryHeader.NOT_FOUND, Unpooled.EMPTY_BUFFER)));
        service.setEntryReaderProvider((topic, streamId, start, end) -> reader(queue));

        assertThatThrownBy(() -> service.materialize(task(
                metadata, deltaResolved(), "default/fail-partition-0", 1L)))
                .isInstanceOf(MaterializationException.class);

        verify(materializer).close();
    }

    @Test
    void materializerFailureConsumesDuplicateWhileServiceReleasesSourceEntry() {
        service.initialize(runtime, config);
        StreamMetadata metadata = metadata("public/default", "owned-failure", Map.of());
        TableMaterializer<GenericEntry> materializer = mock(TableMaterializer.class);
        doAnswer(invocation -> {
            GenericEntry transferred = invocation.getArgument(0);
            transferred.entry().payload().release();
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR, "boom");
        }).when(materializer).write(any(), any());
        registerStubMaterializer(materializer);
        var sourcePayload = Unpooled.buffer().writeByte(1);
        Deque<GenericEntry> queue = new ArrayDeque<>();
        queue.add(new GenericEntry(new Entry(EntryHeader.NOT_FOUND, sourcePayload)));
        service.setEntryReaderProvider((topic, streamId, start, end) -> reader(queue));

        assertThatThrownBy(() -> service.materialize(task(
                metadata, deltaResolved(), "default/owned-failure-partition-0", 1L)))
                .isInstanceOf(MaterializationException.class)
                .hasMessageContaining("boom");

        assertThat(sourcePayload.refCnt()).isZero();
        verify(materializer).close();
    }

    @Test
    void shutdownAfterReaderReturnsEntryStillReleasesOwnedPayload() {
        service.initialize(runtime, config);
        StreamMetadata metadata = metadata("public/default", "shutdown-after-read", Map.of());
        TableMaterializer<GenericEntry> materializer = mock(TableMaterializer.class);
        registerStubMaterializer(materializer);
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

        assertThatThrownBy(() -> service.materialize(task(
                metadata, deltaResolved(), "default/shutdown-after-read-partition-0", 1L)))
                .isInstanceOf(MaterializationException.class)
                .hasMessageContaining("closed");

        assertThat(sourcePayload.refCnt()).isZero();
        verify(materializer, never()).write(any(), any());
        verify(materializer).close();
    }

    @Test
    void materializationServiceProviderLoadsDefaultClassName() {
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

    private void registerStubMaterializer(TableMaterializer<?> materializer) {
        service.registerFactory(TableCatalogType.DELTA, new TableMaterializerFactory() {
            @Override
            public TableCatalogType catalogType() {
                return TableCatalogType.DELTA;
            }

            @Override
            public TableMaterializer<?> create(
                    TableMaterializationPolicy policy,
                    TableCatalog resolvedCatalog,
                    StreamMetadata streamMetadata,
                    MaterializationRuntime materializationRuntime) {
                return materializer;
            }

            @Override
            public TableSchemaService<?, ?> schemaService(
                    TableMaterializationPolicy policy,
                    TableCatalog resolvedCatalog,
                    StreamMetadata streamMetadata) {
                return null;
            }
        });
    }

    private static TableMaterializerFactory blockingFactory(
            CountDownLatch entered,
            CountDownLatch release,
            TableMaterializer<?> materializer) {
        return new TableMaterializerFactory() {
            @Override
            public TableCatalogType catalogType() {
                return TableCatalogType.DELTA;
            }

            @Override
            public TableMaterializer<?> create(
                    TableMaterializationPolicy policy,
                    TableCatalog resolvedCatalog,
                    StreamMetadata streamMetadata,
                    MaterializationRuntime materializationRuntime) {
                entered.countDown();
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("Timed out waiting for factory release");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                return materializer;
            }

            @Override
            public TableSchemaService<?, ?> schemaService(
                    TableMaterializationPolicy policy,
                    TableCatalog resolvedCatalog,
                    StreamMetadata streamMetadata) {
                return null;
            }
        };
    }

    private static EntryReaderProvider emptyReader() {
        return (topic, streamId, start, end) -> reader(new ArrayDeque<>());
    }

    private static IEntryReader reader(Deque<GenericEntry> entries) {
        return new IEntryReader() {
            @Override
            public GenericEntry read() {
                return entries.poll();
            }

            @Override
            public void close() {
            }
        };
    }

    private static CompactStreamTask sourceTask(
            String topic, long streamId, Map<String, String> properties) {
        CompactStreamTask task = new CompactStreamTask();
        task.setTopic(topic);
        task.setTaskName(topic + "-task");
        task.setStreamId(streamId);
        task.setStartOffset(0L);
        task.setEndOffset(0L);
        task.setProperties(properties);
        return task;
    }

    private static MaterializationTask task(
            StreamMetadata metadata,
            ResolvedMaterialization resolved,
            String topic,
            long endOffset) {
        return new MaterializationTask(metadata, resolved, topic, 1L, 0L, endOffset);
    }

    private static StreamMetadata metadata(
            String namespace, String name, Map<String, String> properties) {
        return metadata(StreamIdentifier.of(namespace, name), properties);
    }

    private static StreamMetadata metadata(
            StreamIdentifier identifier, Map<String, String> properties) {
        StreamMetadata metadata = mock(StreamMetadata.class);
        when(metadata.identifier()).thenReturn(identifier);
        when(metadata.properties()).thenReturn(properties);
        return metadata;
    }

    private static ResolvedMaterialization deltaResolved() {
        TableIdentifier identifier = new TableIdentifier("ns", "tbl");
        return new ResolvedMaterialization(
                new TableCatalog("delta-cat", TableCatalogType.DELTA, Map.of(), Map.of()),
                identifier,
                policyWithMode("delta-cat", identifier, TableMode.EXTERNAL));
    }

    private static TableMaterializationPolicy policyWithMode(
            String catalog, TableIdentifier identifier, TableMode mode) {
        return new TableMaterializationPolicy(
                Optional.of(catalog),
                Optional.empty(),
                Optional.of(identifier),
                Optional.of(Boolean.TRUE),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new TableConf(
                        Optional.of(mode),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                Map.of());
    }

    private static final class RecordingMetrics implements MaterializationMetrics {
        private record EvolutionRejection(
                String catalog, TableCatalogType type, StreamIdentifier stream, String reason) {
        }

        private final Deque<EvolutionRejection> evolutionRejections = new ArrayDeque<>();

        @Override
        public void recordWritten(
                String catalog, TableCatalogType catalogType, StreamIdentifier stream) {
        }

        @Override
        public void recordCommitDuration(
                String catalog, TableCatalogType catalogType,
                StreamIdentifier stream, long durationNanos) {
        }

        @Override
        public void recordCommitRetry(
                String catalog, TableCatalogType catalogType,
                StreamIdentifier stream, boolean success) {
        }

        @Override
        public void recordSchemaEvolutionApplied(
                String catalog, TableCatalogType catalogType,
                StreamIdentifier stream, String operation) {
        }

        @Override
        public void recordSchemaEvolutionRejected(
                String catalog, TableCatalogType catalogType,
                StreamIdentifier stream, String reason) {
            evolutionRejections.add(new EvolutionRejection(catalog, catalogType, stream, reason));
        }

        @Override
        public void recordDlqRecord(
                String catalog, TableCatalogType catalogType, StreamIdentifier stream) {
        }

        @Override
        public void setState(
                String catalog, TableCatalogType catalogType,
                StreamIdentifier stream, MaterializationState state) {
        }
    }
}
