/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.lakestream.api.CatalogPaths;
import io.lakestream.api.LifecycleState;
import io.lakestream.api.LogId;
import io.lakestream.api.LogStorage;
import io.lakestream.api.Namespace;
import io.lakestream.api.Partitioning;
import io.lakestream.api.PartitioningStrategy;
import io.lakestream.api.SchemaConfig;
import io.lakestream.api.Stream;
import io.lakestream.api.StreamConfig;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.StreamLayout;
import io.lakestream.api.StreamReader;
import io.lakestream.api.StreamWriter;
import io.lakestream.api.exception.AlreadyExistsException;
import io.lakestream.api.exception.NamespaceNotEmptyException;
import io.lakestream.api.exception.NoSuchNamespaceException;
import io.lakestream.api.exception.NoSuchStreamException;
import io.lakestream.ursa.catalog.metadata.LogMetadata;
import io.lakestream.ursa.catalog.metadata.LogMetadataSerde;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexedStreamCatalogTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final LogMetadataSerde LOG_METADATA_SERDE = LogMetadataSerde.INSTANCE;
    private static final Version DUMMY_VERSION = new Version(1, 0, 0, 0, Optional.empty(), Optional.empty());

    @Mock
    private AsyncOxiaClient oxiaClient;
    @Mock
    private LogStorage logStorage;

    private CatalogPaths catalogPaths;
    private IndexedStreamCatalog catalog;
    private StreamIdentifier streamId;

    private long nextStreamId = 100L;

    @BeforeEach
    void setUp() {
        catalogPaths = new DefaultCatalogPaths();
        catalog = new IndexedStreamCatalog(oxiaClient, catalogPaths, logStorage,
            logId -> null, null,
            key -> CompletableFuture.completedFuture(nextStreamId++),
            null, null, List.of());
        catalog.initialize("test-catalog", Map.of()).join();
        streamId = new StreamIdentifier("public/default", "my-topic");
    }

    // --- createStream ---

    @Test
    void createStream_success() throws Exception {
        nextStreamId = 100L;
        byte[] configBytes;
        try {
            configBytes = MAPPER.writeValueAsBytes(Map.of("partitions", 2));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        when(oxiaClient.get("/admin/streams/public/default/my-topic"))
            .thenReturn(CompletableFuture.completedFuture(null))
            .thenReturn(CompletableFuture.completedFuture(
                new GetResult("/admin/streams/public/default/my-topic", configBytes, DUMMY_VERSION)));

        // Put calls succeed
        when(oxiaClient.put(any(), any(byte[].class)))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", DUMMY_VERSION)));

        // getLayout reads partition metadata to resolve LogIds
        mockPartitionMetadata(streamId, 0, 100L, Map.of("key1", "val1"));
        mockPartitionMetadata(streamId, 1, 101L, Map.of("key1", "val1"));

        Partitioning partitioning = new Partitioning(PartitioningStrategy.INDEXED, Map.of("numPartitions", "2"));
        Stream result = catalog.createStream(streamId, new StreamConfig(), partitioning,
            new SchemaConfig(), Map.of("key1", "val1")).get();

        assertEquals(streamId, result.identifier());
        assertEquals(LifecycleState.ACTIVE, result.state());
        assertEquals(Map.of("key1", "val1"), result.properties());

        // Verify 2 partition metadata puts + 1 config put = 3 total
        verify(oxiaClient, times(3)).put(any(), any(byte[].class));
        verify(oxiaClient).put(eq("/streams/public/default/my-topic-partition-0"), any(byte[].class));
        verify(oxiaClient).put(eq("/streams/public/default/my-topic-partition-1"), any(byte[].class));
        verify(oxiaClient).put(eq("/admin/streams/public/default/my-topic"), any(byte[].class));
    }

    @Test
    void createStream_alreadyExists() {
        when(oxiaClient.get("/admin/streams/public/default/my-topic"))
            .thenReturn(CompletableFuture.completedFuture(
                new GetResult("/admin/streams/public/default/my-topic", new byte[]{}, DUMMY_VERSION)));

        Partitioning partitioning = new Partitioning(PartitioningStrategy.INDEXED, Map.of("numPartitions", "1"));
        ExecutionException ex = assertThrows(ExecutionException.class, () ->
            catalog.createStream(streamId, new StreamConfig(), partitioning,
                new SchemaConfig(), Map.of()).get());
        assertInstanceOf(AlreadyExistsException.class, ex.getCause());
    }

    // --- getLayout ---

    @Test
    void getLayout_success() throws Exception {
        // Partition count = 2
        mockStreamConfig(streamId, 2);

        // Partition 0 has streamId=100, partition 1 has streamId=101
        mockPartitionMetadata(streamId, 0, 100L, Map.of());
        mockPartitionMetadata(streamId, 1, 101L, Map.of());

        StreamLayout layout = catalog.getLayout(streamId).get();
        assertNotNull(layout);
        assertEquals(2, layout.logCount());

        List<LogId> logIds = layout.logIds().get();
        assertEquals(LogId.of(100), logIds.get(0));
        assertEquals(LogId.of(101), logIds.get(1));
    }

    // --- loadStream ---

    @Test
    void loadStream_success() throws Exception {
        mockStreamConfig(streamId, 3, Map.of("env", "prod"));
        // getLayout reads all partition metadata for log IDs
        mockPartitionMetadata(streamId, 0, 200L, Map.of());
        mockPartitionMetadata(streamId, 1, 201L, Map.of());
        mockPartitionMetadata(streamId, 2, 202L, Map.of());

        Stream stream = catalog.loadStream(streamId).get();
        assertEquals(streamId, stream.identifier());
        assertEquals(3, stream.partitioning().numPartitions());
        assertEquals(Map.of("env", "prod"), stream.properties());
    }

    @Test
    void loadStream_notFound() {
        when(oxiaClient.get("/admin/streams/public/default/my-topic"))
            .thenReturn(CompletableFuture.completedFuture(null));

        ExecutionException ex = assertThrows(ExecutionException.class, () ->
            catalog.loadStream(streamId).get());
        assertInstanceOf(NoSuchStreamException.class, ex.getCause());
    }

    @Test
    void loadStream_tolerantOfNotYetRegisteredSiblingPartitions() throws Exception {
        // External partitions register lazily (one per compaction task) and the count grows to the
        // highest index seen — so partition 2 can be registered while 0 and 1 are not yet written.
        // loadStream (used only by the materialization worker, which never touches the layout) must
        // still succeed so materializing partition 2 does not wait on its siblings being compacted.
        mockStreamConfig(streamId, 3, Map.of("env", "prod"));
        mockPartitionMetadata(streamId, 2, 202L, Map.of());
        when(oxiaClient.get(catalogPaths.partitionMetadataPath(streamId, 0)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(oxiaClient.get(catalogPaths.partitionMetadataPath(streamId, 1)))
            .thenReturn(CompletableFuture.completedFuture(null));

        Stream stream = catalog.loadStream(streamId).get();
        assertEquals(streamId, stream.identifier());
        assertEquals(3, stream.partitioning().numPartitions());
        assertEquals(Map.of("env", "prod"), stream.properties());
    }

    @Test
    void getLayout_strictlyRequiresAllPartitions() {
        // The public getLayout stays strict: native readers need a complete layout, so a missing
        // partition surfaces as NoSuchStreamException rather than a placeholder.
        mockStreamConfig(streamId, 3, Map.of());
        mockPartitionMetadata(streamId, 2, 202L, Map.of());
        when(oxiaClient.get(catalogPaths.partitionMetadataPath(streamId, 0)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(oxiaClient.get(catalogPaths.partitionMetadataPath(streamId, 1)))
            .thenReturn(CompletableFuture.completedFuture(null));

        ExecutionException ex = assertThrows(ExecutionException.class, () ->
            catalog.getLayout(streamId).get());
        assertInstanceOf(NoSuchStreamException.class, ex.getCause());
    }

    // --- listStreams ---

    @Test
    void listStreams_success() throws Exception {
        String prefix = "/admin/streams/public/default/";
        when(oxiaClient.list(eq(prefix), eq(prefix + "\uffff")))
            .thenReturn(CompletableFuture.completedFuture(List.of(
                "/admin/streams/public/default/topic-a",
                "/admin/streams/public/default/topic-b"
            )));

        List<StreamIdentifier> streams = catalog.listStreams("public/default").get();
        assertEquals(2, streams.size());
        assertEquals("topic-a", streams.get(0).name());
        assertEquals("topic-b", streams.get(1).name());
    }

    // --- streamExists ---

    @Test
    void streamExists_true() throws Exception {
        when(oxiaClient.get("/admin/streams/public/default/my-topic"))
            .thenReturn(CompletableFuture.completedFuture(
                new GetResult("key", new byte[]{}, DUMMY_VERSION)));
        assertTrue(catalog.streamExists(streamId).get());
    }

    @Test
    void streamExists_false() throws Exception {
        when(oxiaClient.get("/admin/streams/public/default/my-topic"))
            .thenReturn(CompletableFuture.completedFuture(null));
        assertFalse(catalog.streamExists(streamId).get());
    }

    // --- dropStream ---

    @Test
    void dropStream_success() throws Exception {
        mockStreamExistence(streamId, true);

        when(oxiaClient.delete(any()))
            .thenReturn(CompletableFuture.completedFuture(true));

        boolean result = catalog.dropStream(streamId, false).get();
        assertTrue(result);
        verify(oxiaClient).delete("/streams/public/default/my-topic-partition-0");
        verify(oxiaClient).delete("/admin/streams/public/default/my-topic");
        verify(logStorage, never()).deleteLog(any());
    }

    @Test
    void dropStream_withPurge() throws Exception {
        mockStreamExistence(streamId, true);
        mockStreamConfig(streamId, 1);
        mockPartitionMetadata(streamId, 0, 300L, Map.of());

        when(oxiaClient.delete(any()))
            .thenReturn(CompletableFuture.completedFuture(true));
        when(logStorage.deleteLog(LogId.of(300L)))
            .thenReturn(CompletableFuture.completedFuture(null));

        boolean result = catalog.dropStream(streamId, true).get();
        assertTrue(result);
        verify(logStorage).deleteLog(LogId.of(300L));
    }

    @Test
    void dropStream_notFound() throws Exception {
        mockStreamExistence(streamId, false);
        assertFalse(catalog.dropStream(streamId, false).get());
    }

    // --- Namespace CRUD ---

    @Test
    void createNamespace_success() throws Exception {
        when(oxiaClient.get("/admin/streams/_namespaces/my-ns"))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(oxiaClient.put(any(), any(byte[].class)))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", DUMMY_VERSION)));

        catalog.createNamespace(new Namespace("my-ns", Map.of("key", "val"))).get();

        verify(oxiaClient).put(eq("/admin/streams/_namespaces/my-ns"), any(byte[].class));
    }

    @Test
    void createNamespace_alreadyExists() {
        when(oxiaClient.get("/admin/streams/_namespaces/my-ns"))
            .thenReturn(CompletableFuture.completedFuture(
                new GetResult("key", new byte[]{}, DUMMY_VERSION)));

        ExecutionException ex = assertThrows(ExecutionException.class, () ->
            catalog.createNamespace(new Namespace("my-ns")).get());
        assertInstanceOf(AlreadyExistsException.class, ex.getCause());
    }

    @Test
    void listNamespaces_success() throws Exception {
        String prefix = "/admin/streams/_namespaces/";
        when(oxiaClient.list(eq(prefix), eq(prefix + "\uffff")))
            .thenReturn(CompletableFuture.completedFuture(List.of(
                "/admin/streams/_namespaces/ns-a",
                "/admin/streams/_namespaces/ns-b")));

        mockNamespaceMetadata("ns-a", Map.of());
        mockNamespaceMetadata("ns-b", Map.of("x", "y"));

        List<Namespace> namespaces = catalog.listNamespaces().get();
        assertEquals(2, namespaces.size());
        assertEquals("ns-a", namespaces.get(0).name());
        assertEquals("ns-b", namespaces.get(1).name());
    }

    @Test
    void loadNamespaceMetadata_notFound() {
        when(oxiaClient.get("/admin/streams/_namespaces/missing-ns"))
            .thenReturn(CompletableFuture.completedFuture(null));

        CompletionException ex = assertThrows(CompletionException.class, () ->
            catalog.loadNamespaceMetadata("missing-ns").join());
        assertInstanceOf(NoSuchNamespaceException.class, ex.getCause());
    }

    @Test
    void namespaceExists_true() throws Exception {
        when(oxiaClient.get("/admin/streams/_namespaces/my-ns"))
            .thenReturn(CompletableFuture.completedFuture(
                new GetResult("key", new byte[]{}, DUMMY_VERSION)));
        assertTrue(catalog.namespaceExists("my-ns").get());
    }

    @Test
    void namespaceExists_false() throws Exception {
        when(oxiaClient.get("/admin/streams/_namespaces/my-ns"))
            .thenReturn(CompletableFuture.completedFuture(null));
        assertFalse(catalog.namespaceExists("my-ns").get());
    }

    @Test
    void dropNamespace_success() throws Exception {
        mockNamespaceMetadata("my-ns", Map.of());
        // namespace has no streams — listStreams now scans streamConfigPrefix
        String configPrefix = "/admin/streams/my-ns/";
        when(oxiaClient.list(eq(configPrefix), eq(configPrefix + "\uffff")))
            .thenReturn(CompletableFuture.completedFuture(List.of()));
        when(oxiaClient.delete("/admin/streams/_namespaces/my-ns"))
            .thenReturn(CompletableFuture.completedFuture(true));

        assertTrue(catalog.dropNamespace("my-ns").get());
        verify(oxiaClient).delete("/admin/streams/_namespaces/my-ns");
    }

    @Test
    void dropNamespace_notEmpty() {
        mockNamespaceMetadata("my-ns", Map.of());
        String configPrefix = "/admin/streams/my-ns/";
        when(oxiaClient.list(eq(configPrefix), eq(configPrefix + "\uffff")))
            .thenReturn(CompletableFuture.completedFuture(
                List.of("/admin/streams/my-ns/some-topic")));

        ExecutionException ex = assertThrows(ExecutionException.class, () ->
            catalog.dropNamespace("my-ns").get());
        assertInstanceOf(NamespaceNotEmptyException.class, ex.getCause());
    }

    @Test
    void dropNamespace_notFound() throws Exception {
        when(oxiaClient.get("/admin/streams/_namespaces/missing-ns"))
            .thenReturn(CompletableFuture.completedFuture(null));
        assertFalse(catalog.dropNamespace("missing-ns").get());
    }

    // --- Edge cases ---

    @Test
    void singlePartitionStream() throws Exception {
        mockStreamConfig(streamId, 1);
        mockPartitionMetadata(streamId, 0, 500L, Map.of());

        StreamLayout layout = catalog.getLayout(streamId).get();
        assertEquals(1, layout.logCount());
        List<LogId> logIds = layout.logIds().get();
        assertEquals(LogId.of(500), logIds.get(0));
    }

    @Test
    void name_returnsInitializedName() {
        assertEquals("test-catalog", catalog.name());
    }

    // --- setStreamProperties ---

    @Test
    void setStreamProperties_success() throws Exception {
        mockStreamConfig(streamId, 2, Map.of("a", "1"));

        when(oxiaClient.put(any(), any(byte[].class), any()))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", DUMMY_VERSION)));

        catalog.setStreamProperties(streamId, Map.of("b", "2")).get();

        // Properties are now stored at stream config level, not per-partition
        String configPath = catalogPaths.streamConfigPath(streamId);
        verify(oxiaClient).put(eq(configPath), any(byte[].class),
            eq(Set.of(PutOption.IfVersionIdEquals(DUMMY_VERSION.versionId()))));
    }

    // --- removeStreamProperties ---

    @Test
    void removeStreamProperties_success() throws Exception {
        mockStreamConfig(streamId, 1, Map.of("a", "1", "b", "2"));

        when(oxiaClient.put(any(), any(byte[].class), any()))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", DUMMY_VERSION)));

        catalog.removeStreamProperties(streamId, List.of("a")).get();

        // Properties are now stored at stream config level, not per-partition
        String configPath = catalogPaths.streamConfigPath(streamId);
        verify(oxiaClient).put(eq(configPath), any(byte[].class),
            eq(Set.of(PutOption.IfVersionIdEquals(DUMMY_VERSION.versionId()))));
    }

    @Test
    void setStreamProperties_retriesWithoutLosingConcurrentPartitionGrowth() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        Version grownVersion = new Version(2, 0, 0, 0, Optional.empty(), Optional.empty());
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                configPath, streamConfigBytes(1, Map.of("a", "1"), false), DUMMY_VERSION)))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                configPath, streamConfigBytes(4, Map.of("a", "1"), true), grownVersion)));
        when(oxiaClient.put(eq(configPath), any(byte[].class), any()))
            .thenReturn(CompletableFuture.failedFuture(
                new UnexpectedVersionIdException(configPath, DUMMY_VERSION.versionId())))
            .thenReturn(CompletableFuture.completedFuture(new PutResult(configPath, grownVersion)));

        catalog.setStreamProperties(streamId, Map.of("b", "2")).get();

        ArgumentCaptor<byte[]> writes = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient, times(2)).put(eq(configPath), writes.capture(), any());
        JsonNode retried = MAPPER.readTree(writes.getAllValues().get(1));
        assertEquals(4, retried.get("partitions").asInt());
        assertEquals("1", retried.get("properties").get("a").asText());
        assertEquals("2", retried.get("properties").get("b").asText());
        assertTrue(retried.get("materialization").get("enabled").asBoolean());
    }

    @Test
    void removeStreamProperties_retriesAgainstLatestPropertiesAndPartitionCount() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        Version grownVersion = new Version(2, 0, 0, 0, Optional.empty(), Optional.empty());
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                configPath, streamConfigBytes(1, Map.of("a", "1", "b", "2"), false),
                DUMMY_VERSION)))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                configPath, streamConfigBytes(3, Map.of("a", "1", "b", "2", "c", "3"), false),
                grownVersion)));
        when(oxiaClient.put(eq(configPath), any(byte[].class), any()))
            .thenReturn(CompletableFuture.failedFuture(
                new UnexpectedVersionIdException(configPath, DUMMY_VERSION.versionId())))
            .thenReturn(CompletableFuture.completedFuture(new PutResult(configPath, grownVersion)));

        catalog.removeStreamProperties(streamId, List.of("a")).get();

        ArgumentCaptor<byte[]> writes = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient, times(2)).put(eq(configPath), writes.capture(), any());
        JsonNode retried = MAPPER.readTree(writes.getAllValues().get(1));
        assertEquals(3, retried.get("partitions").asInt());
        assertFalse(retried.get("properties").has("a"));
        assertEquals("2", retried.get("properties").get("b").asText());
        assertEquals("3", retried.get("properties").get("c").asText());
    }

    @Test
    void clearStreamMaterialization_retriesWithoutLosingConcurrentConfigChanges() throws Exception {
        String configPath = catalogPaths.streamConfigPath(streamId);
        Version grownVersion = new Version(2, 0, 0, 0, Optional.empty(), Optional.empty());
        when(oxiaClient.get(configPath))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                configPath, streamConfigBytes(1, Map.of("owner", "old"), true), DUMMY_VERSION)))
            .thenReturn(CompletableFuture.completedFuture(new GetResult(
                configPath, streamConfigBytes(5, Map.of("owner", "latest"), true), grownVersion)));
        when(oxiaClient.put(eq(configPath), any(byte[].class), any()))
            .thenReturn(CompletableFuture.failedFuture(
                new UnexpectedVersionIdException(configPath, DUMMY_VERSION.versionId())))
            .thenReturn(CompletableFuture.completedFuture(new PutResult(configPath, grownVersion)));

        catalog.clearStreamMaterialization(streamId).get();

        ArgumentCaptor<byte[]> writes = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient, times(2)).put(eq(configPath), writes.capture(), any());
        JsonNode retried = MAPPER.readTree(writes.getAllValues().get(1));
        assertEquals(5, retried.get("partitions").asInt());
        assertEquals("latest", retried.get("properties").get("owner").asText());
        assertFalse(retried.has("materialization"));
    }

    // --- setNamespaceProperties ---

    @Test
    void setNamespaceProperties_success() throws Exception {
        mockNamespaceMetadata("my-ns", Map.of("a", "1"));

        when(oxiaClient.put(any(), any(byte[].class)))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", DUMMY_VERSION)));

        catalog.setNamespaceProperties("my-ns", Map.of("b", "2")).get();

        verify(oxiaClient).put(eq("/admin/streams/_namespaces/my-ns"), any(byte[].class));
    }

    @Test
    void setNamespaceProperties_notFound() {
        when(oxiaClient.get("/admin/streams/_namespaces/missing-ns"))
            .thenReturn(CompletableFuture.completedFuture(null));

        CompletionException ex = assertThrows(CompletionException.class, () ->
            catalog.setNamespaceProperties("missing-ns", Map.of("a", "1")).join());
        assertInstanceOf(NoSuchNamespaceException.class, ex.getCause());
    }

    // --- removeNamespaceProperties ---

    @Test
    void removeNamespaceProperties_success() throws Exception {
        mockNamespaceMetadata("my-ns", Map.of("a", "1", "b", "2"));

        when(oxiaClient.put(any(), any(byte[].class)))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", DUMMY_VERSION)));

        catalog.removeNamespaceProperties("my-ns", List.of("a")).get();

        verify(oxiaClient).put(eq("/admin/streams/_namespaces/my-ns"), any(byte[].class));
    }

    @Test
    void removeNamespaceProperties_notFound() {
        when(oxiaClient.get("/admin/streams/_namespaces/missing-ns"))
            .thenReturn(CompletableFuture.completedFuture(null));

        CompletionException ex = assertThrows(CompletionException.class, () ->
            catalog.removeNamespaceProperties("missing-ns", List.of("a")).join());
        assertInstanceOf(NoSuchNamespaceException.class, ex.getCause());
    }

    // --- sealStream ---

    @Test
    void sealStream_success() throws Exception {
        mockStreamConfig(streamId, 2);
        mockPartitionMetadata(streamId, 0, 100L, Map.of());
        mockPartitionMetadata(streamId, 1, 101L, Map.of());

        when(oxiaClient.put(any(), any(byte[].class)))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", DUMMY_VERSION)));

        catalog.sealStream(streamId).get();

        verify(oxiaClient).put(eq("/streams/public/default/my-topic-partition-0"), any(byte[].class));
        verify(oxiaClient).put(eq("/streams/public/default/my-topic-partition-1"), any(byte[].class));
    }

    @Test
    void sealStream_streamNotFound() {
        when(oxiaClient.get("/admin/streams/public/default/my-topic"))
            .thenReturn(CompletableFuture.completedFuture(null));

        ExecutionException ex = assertThrows(ExecutionException.class, () ->
            catalog.sealStream(streamId).get());
        assertInstanceOf(NoSuchStreamException.class, ex.getCause());
    }

    // --- truncateStream ---

    @Test
    void truncateStream_success() throws Exception {
        mockStreamConfig(streamId, 2);
        mockPartitionMetadata(streamId, 0, 100L, Map.of());
        mockPartitionMetadata(streamId, 1, 101L, Map.of());

        when(logStorage.deleteLog(any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        catalog.truncateStream(streamId).get();

        verify(logStorage).deleteLog(LogId.of(100L));
        verify(logStorage).deleteLog(LogId.of(101L));
    }

    @Test
    void truncateStream_streamNotFound() {
        when(oxiaClient.get("/admin/streams/public/default/my-topic"))
            .thenReturn(CompletableFuture.completedFuture(null));

        ExecutionException ex = assertThrows(ExecutionException.class, () ->
            catalog.truncateStream(streamId).get());
        assertInstanceOf(NoSuchStreamException.class, ex.getCause());
    }

    // --- openWriter ---

    @Test
    void openWriter_success() throws Exception {
        mockStreamConfig(streamId, 1);
        mockPartitionMetadata(streamId, 0, 100L, Map.of());

        StreamWriter writer = catalog.openWriter(streamId).get();
        assertNotNull(writer);
        assertInstanceOf(StreamWriterImpl.class, writer);
    }

    @Test
    void openWriter_streamNotFound() {
        when(oxiaClient.get("/admin/streams/public/default/my-topic"))
            .thenReturn(CompletableFuture.completedFuture(null));

        ExecutionException ex = assertThrows(ExecutionException.class, () ->
            catalog.openWriter(streamId).get());
        assertInstanceOf(NoSuchStreamException.class, ex.getCause());
    }

    // --- openReader ---

    @Test
    void openReader_success() throws Exception {
        mockStreamConfig(streamId, 1);
        mockPartitionMetadata(streamId, 0, 100L, Map.of());

        StreamReader reader = catalog.openReader(streamId).get();
        assertNotNull(reader);
        assertInstanceOf(StreamReaderImpl.class, reader);
    }

    @Test
    void openReader_streamNotFound() {
        when(oxiaClient.get("/admin/streams/public/default/my-topic"))
            .thenReturn(CompletableFuture.completedFuture(null));

        ExecutionException ex = assertThrows(ExecutionException.class, () ->
            catalog.openReader(streamId).get());
        assertInstanceOf(NoSuchStreamException.class, ex.getCause());
    }

    // --- Helpers ---

    private void mockStreamConfig(StreamIdentifier id, int numPartitions) {
        mockStreamConfig(id, numPartitions, Map.of());
    }

    private void mockStreamConfig(StreamIdentifier id, int numPartitions,
                                    Map<String, String> properties) {
        String configPath = catalogPaths.streamConfigPath(id);
        try {
            byte[] bytes = MAPPER.writeValueAsBytes(
                Map.of("partitions", numPartitions, "properties", properties));
            when(oxiaClient.get(configPath))
                .thenReturn(CompletableFuture.completedFuture(
                    new GetResult(configPath, bytes, DUMMY_VERSION)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] streamConfigBytes(
            int partitions, Map<String, String> properties, boolean materialization)
            throws Exception {
        ObjectNode config = MAPPER.createObjectNode();
        config.put("partitions", partitions);
        ObjectNode configProperties = config.putObject("properties");
        properties.forEach(configProperties::put);
        if (materialization) {
            config.putObject("materialization").put("enabled", true);
        }
        return MAPPER.writeValueAsBytes(config);
    }

    private void mockPartitionMetadata(StreamIdentifier id, int partIdx, long partStreamId,
                                        Map<String, String> properties) {
        String path = catalogPaths.partitionMetadataPath(id, partIdx);
        LogMetadata metadata = new LogMetadata(partStreamId, properties, OptionalLong.empty());
        try {
            byte[] bytes = LOG_METADATA_SERDE.serialize(path, metadata);
            when(oxiaClient.get(path))
                .thenReturn(CompletableFuture.completedFuture(
                    new GetResult(path, bytes, DUMMY_VERSION)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void mockStreamExistence(StreamIdentifier id, boolean exists) {
        String configPath = catalogPaths.streamConfigPath(id);
        if (exists) {
            try {
                byte[] bytes = MAPPER.writeValueAsBytes(
                    Map.of("partitions", 1, "properties", Map.of()));
                when(oxiaClient.get(configPath))
                    .thenReturn(CompletableFuture.completedFuture(
                        new GetResult(configPath, bytes, DUMMY_VERSION)));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            when(oxiaClient.get(configPath))
                .thenReturn(CompletableFuture.completedFuture(null));
        }
    }

    private void mockNamespaceMetadata(String nsName, Map<String, String> props) {
        String path = catalogPaths.namespacePath(nsName);
        try {
            byte[] bytes = MAPPER.writeValueAsBytes(props);
            when(oxiaClient.get(path))
                .thenReturn(CompletableFuture.completedFuture(
                    new GetResult(path, bytes, DUMMY_VERSION)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // --- registerExternalStream (externally controlled logical streams) ---

    @Test
    void registerExternalStream_createsOnlyLogicalConfigWhenAbsent() throws Exception {
        String configPath = "/admin/streams/public/default/my-topic";
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(null));
        when(oxiaClient.put(any(), any(byte[].class), any()))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", DUMMY_VERSION)));

        catalog.registerExternalStream(streamId, 3, Map.of("owner", "kafka")).get();

        ArgumentCaptor<byte[]> config = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient).put(eq(configPath), config.capture(),
            eq(Set.of(PutOption.IfRecordDoesNotExist)));
        verify(oxiaClient, times(1)).put(any(), any(byte[].class), any());
        verify(logStorage, never()).deleteLog(any());

        JsonNode node = MAPPER.readTree(config.getValue());
        assertEquals(3, node.get("partitions").asInt());
        assertEquals("kafka", node.get("properties").get("owner").asText());
        assertFalse(node.has("materialization"));
    }

    @Test
    void registerExternalStream_rejectsNonPositivePartitionCount() {
        assertThrows(IllegalArgumentException.class,
            () -> catalog.registerExternalStream(streamId, 0, Map.of()));
    }

    @Test
    void registerExternalStream_growsWithCasAndPreservesExistingConfig() throws Exception {
        String configPath = "/admin/streams/public/default/my-topic";
        ObjectNode existing = MAPPER.createObjectNode();
        existing.put("partitions", 2);
        existing.putObject("properties").put("owner", "existing");
        existing.putObject("materialization")
            .put("enabled", true)
            .putObject("connectionOverrides");
        byte[] existingBytes = MAPPER.writeValueAsBytes(existing);
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(configPath, existingBytes, DUMMY_VERSION)));
        when(oxiaClient.put(any(), any(byte[].class), any()))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", DUMMY_VERSION)));

        catalog.registerExternalStream(streamId, 4, Map.of("owner", "replacement")).get();

        ArgumentCaptor<byte[]> config = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient).put(eq(configPath), config.capture(),
            eq(Set.of(PutOption.IfVersionIdEquals(DUMMY_VERSION.versionId()))));
        JsonNode node = MAPPER.readTree(config.getValue());
        assertEquals(4, node.get("partitions").asInt());
        assertEquals("existing", node.get("properties").get("owner").asText());
        assertTrue(node.get("materialization").get("enabled").asBoolean());
    }

    @Test
    void registerExternalStream_doesNotShrinkExistingConfig() throws Exception {
        String configPath = "/admin/streams/public/default/my-topic";
        ObjectNode existing = MAPPER.createObjectNode();
        existing.put("partitions", 5);
        existing.putObject("properties").put("owner", "existing");
        byte[] existingBytes = MAPPER.writeValueAsBytes(existing);
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(configPath, existingBytes, DUMMY_VERSION)));

        catalog.registerExternalStream(streamId, 3, Map.of("owner", "replacement")).get();

        verify(oxiaClient, never()).put(any(), any(byte[].class), any());
    }

    @Test
    void unregisterExternalStream_deletesOnlyLogicalConfigAndIsIdempotent() throws Exception {
        String configPath = "/admin/streams/public/default/my-topic";
        when(oxiaClient.delete(configPath))
            .thenReturn(CompletableFuture.completedFuture(true))
            .thenReturn(CompletableFuture.completedFuture(false));

        catalog.unregisterExternalStream(streamId).get();
        catalog.unregisterExternalStream(streamId).get();

        verify(oxiaClient, times(2)).delete(configPath);
        verify(oxiaClient, never()).delete("/streams/public/default/my-topic-partition-0");
        verify(logStorage, never()).deleteLog(any());
    }

    // --- registerExternalPartition (broker-created streams) ---

    @Test
    void registerExternalPartition_createsConfigWhenAbsent() throws Exception {
        String configPath = "/admin/streams/public/default/my-topic";
        String partPath = "/streams/public/default/my-topic-partition-0";
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(null));
        when(oxiaClient.put(any(), any(byte[].class), any()))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", DUMMY_VERSION)));

        catalog.registerExternalPartition(streamId, 0, 100L, Map.of("k", "v")).get();

        // Partition metadata is written create-only (must not clobber the broker's real metadata).
        verify(oxiaClient).put(eq(partPath), any(byte[].class), eq(Set.of(PutOption.IfRecordDoesNotExist)));
        ArgumentCaptor<byte[]> cfg = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient).put(eq(configPath), cfg.capture(), eq(Set.of(PutOption.IfRecordDoesNotExist)));
        JsonNode node = MAPPER.readTree(cfg.getValue());
        assertEquals(1, node.get("partitions").asInt());
    }

    @Test
    void registerExternalPartition_growsPartitionCountWithCas() throws Exception {
        String configPath = "/admin/streams/public/default/my-topic";
        ObjectNode existing = MAPPER.createObjectNode();
        existing.put("partitions", 1);
        existing.putObject("properties");
        byte[] existingBytes = MAPPER.writeValueAsBytes(existing);
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(configPath, existingBytes, DUMMY_VERSION)));
        when(oxiaClient.put(any(), any(byte[].class), any()))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", DUMMY_VERSION)));

        catalog.registerExternalPartition(streamId, 2, 100L, Map.of()).get();

        // The grow is a version-guarded compare-and-set; partition count becomes index + 1.
        ArgumentCaptor<byte[]> cfg = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient).put(eq(configPath), cfg.capture(),
            eq(Set.of(PutOption.IfVersionIdEquals(DUMMY_VERSION.versionId()))));
        JsonNode node = MAPPER.readTree(cfg.getValue());
        assertEquals(3, node.get("partitions").asInt());
    }

    @Test
    void registerExternalPartition_doesNotRestoreStalePropertiesWhenGrowing() throws Exception {
        String configPath = "/admin/streams/public/default/my-topic";
        ObjectNode existing = MAPPER.createObjectNode();
        existing.put("partitions", 1);
        existing.putObject("properties");
        byte[] existingBytes = MAPPER.writeValueAsBytes(existing);
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(configPath, existingBytes, DUMMY_VERSION)));
        when(oxiaClient.put(any(), any(byte[].class), any()))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", DUMMY_VERSION)));

        catalog.registerExternalPartition(
            streamId, 2, 100L, Map.of("retention.ms", "stale-value")).get();

        ArgumentCaptor<byte[]> config = ArgumentCaptor.forClass(byte[].class);
        verify(oxiaClient).put(eq(configPath), config.capture(),
            eq(Set.of(PutOption.IfVersionIdEquals(DUMMY_VERSION.versionId()))));
        JsonNode node = MAPPER.readTree(config.getValue());
        assertEquals(3, node.get("partitions").asInt());
        assertTrue(node.get("properties").isEmpty());
    }

    @Test
    void registerExternalPartition_noopWhenCountAlreadySufficient() throws Exception {
        String configPath = "/admin/streams/public/default/my-topic";
        ObjectNode existing = MAPPER.createObjectNode();
        existing.put("partitions", 5);
        existing.putObject("properties");
        byte[] existingBytes = MAPPER.writeValueAsBytes(existing);
        when(oxiaClient.get(configPath)).thenReturn(CompletableFuture.completedFuture(
            new GetResult(configPath, existingBytes, DUMMY_VERSION)));
        when(oxiaClient.put(any(), any(byte[].class), any()))
            .thenReturn(CompletableFuture.completedFuture(new PutResult("key", DUMMY_VERSION)));

        catalog.registerExternalPartition(streamId, 2, 100L, Map.of()).get();

        // Only the partition metadata is written; the config is already large enough.
        verify(oxiaClient).put(eq("/streams/public/default/my-topic-partition-2"),
            any(byte[].class), eq(Set.of(PutOption.IfRecordDoesNotExist)));
        verify(oxiaClient, never()).put(eq(configPath), any(byte[].class),
            eq(Set.of(PutOption.IfVersionIdEquals(DUMMY_VERSION.versionId()))));
    }
}
