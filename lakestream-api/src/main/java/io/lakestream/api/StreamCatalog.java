/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import io.lakestream.api.materialization.ResolvedMaterialization;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Level 2 interface: stream catalog for metadata management.
 *
 * <p>Provides CRUD operations for namespaces and streams. Metadata operations return immutable
 * snapshots; data-plane handles are opened explicitly through {@link #openLog},
 * {@link #openWriter}, or {@link #openReader}.
 * Every returned data-plane handle must be closed before its catalog is closed.
 *
 * <p>Implementations:
 * <ul>
 *   <li>{@code IndexedStreamCatalog} — Oxia-backed catalog using indexed partitions</li>
 * </ul>
 *
 * <p>Thread safety: implementations must be safe for concurrent use.
 */
public interface StreamCatalog extends AutoCloseable {

    /**
     * Returns the catalog name.
     *
     * @return the catalog name
     */
    String name();

    /**
     * Initializes the catalog with the given name and properties.
     *
     * @param name the catalog name
     * @param properties initialization properties
     * @return a future that completes when initialization is done
     */
    CompletableFuture<Void> initialize(String name, Map<String, String> properties);

    // --- Table catalog operations ---

    /**
     * Registers a {@link TableCatalog} that streams can materialize into.
     *
     * @param catalog the catalog to register
     * @return a future that completes when the catalog is registered
     * @throws io.lakestream.api.exception.AlreadyExistsException if a catalog with the same name exists
     */
    CompletableFuture<Void> registerTableCatalog(TableCatalog catalog);

    /**
     * Unregisters a {@link TableCatalog} by name.
     *
     * @param name the catalog name
     * @return a future resolving to true if the catalog was removed, false if it did not exist
     */
    CompletableFuture<Boolean> unregisterTableCatalog(String name);

    /**
     * Loads a registered {@link TableCatalog} by name.
     *
     * @param name the catalog name
     * @return a future resolving to the catalog
     */
    CompletableFuture<TableCatalog> getTableCatalog(String name);

    /**
     * Lists all registered {@link TableCatalog}s.
     *
     * @return a future resolving to the list of registered catalogs
     */
    CompletableFuture<List<TableCatalog>> listTableCatalogs();

    // --- Namespace operations ---

    /**
     * Creates a new namespace.
     *
     * @param namespace the namespace to create
     * @return a future that completes when the namespace is created
     * @throws io.lakestream.api.exception.AlreadyExistsException if the namespace already exists
     */
    CompletableFuture<Void> createNamespace(Namespace namespace);

    /**
     * Lists all namespaces in this catalog.
     *
     * @return a future resolving to the list of namespaces
     */
    CompletableFuture<List<Namespace>> listNamespaces();

    /**
     * Loads metadata for a specific namespace.
     *
     * @param namespaceName the namespace name
     * @return a future resolving to the namespace metadata
     * @throws io.lakestream.api.exception.NoSuchNamespaceException if not found
     */
    CompletableFuture<Namespace> loadNamespaceMetadata(String namespaceName);

    /**
     * Drops a namespace.
     *
     * @param namespaceName the namespace to drop
     * @return a future resolving to true if the namespace was dropped, false if it didn't exist
     * @throws io.lakestream.api.exception.NamespaceNotEmptyException
     *     if the namespace contains streams
     */
    CompletableFuture<Boolean> dropNamespace(String namespaceName);

    /**
     * Checks whether a namespace exists.
     *
     * @param namespaceName the namespace to check
     * @return a future resolving to true if the namespace exists
     */
    CompletableFuture<Boolean> namespaceExists(String namespaceName);

    /**
     * Sets properties on a namespace (merge semantics).
     *
     * @param name the namespace name
     * @param props the properties to set
     * @return a future that completes when the properties are updated
     */
    CompletableFuture<Void> setNamespaceProperties(String name, Map<String, String> props);

    /**
     * Removes properties from a namespace.
     *
     * @param name the namespace name
     * @param keys the property keys to remove
     * @return a future that completes when the properties are removed
     */
    CompletableFuture<Void> removeNamespaceProperties(String name, List<String> keys);

    /**
     * Sets the namespace-level materialization policy (active baseline).
     *
     * @param namespace the namespace name
     * @param policy the policy to apply
     * @return a future that completes when the policy is set
     */
    CompletableFuture<Void> setNamespaceMaterialization(String namespace, TableMaterializationPolicy policy);

    /**
     * Clears the namespace-level materialization policy.
     *
     * @param namespace the namespace name
     * @return a future that completes when the policy is cleared
     */
    CompletableFuture<Void> clearNamespaceMaterialization(String namespace);

    /**
     * Sets a cluster-wide default materialization policy. This is the lowest-priority baseline:
     * {@link #resolveMaterialization(StreamIdentifier)} resolves a stream policy first, then its
     * namespace policy, and finally this cluster default. It lets
     * {@code materializationEnabled=true} (with no {@code materializationDefaultNamespace})
     * materialize every stream in every namespace without per-namespace authoring.
     *
     * @param policy the cluster-wide default policy
     * @return a future that completes when the policy is set
     */
    default CompletableFuture<Void> setClusterDefaultMaterialization(TableMaterializationPolicy policy) {
        throw new UnsupportedOperationException(
                "cluster-default materialization is not supported by this catalog");
    }

    /**
     * Returns the cluster-wide default materialization policy, or {@link Optional#empty()} if none
     * is set. Used as the lowest-priority fallback when resolving
     * {@link #resolveMaterialization(StreamIdentifier)}.
     *
     * @return the cluster-wide default policy, if any
     */
    default Optional<TableMaterializationPolicy> clusterDefaultMaterialization() {
        return Optional.empty();
    }

    // --- Stream operations ---

    /**
     * Lists all streams in a namespace.
     *
     * @param namespaceName the namespace to list streams from
     * @return a future resolving to the list of stream identifiers
     */
    CompletableFuture<List<StreamIdentifier>> listStreams(String namespaceName);

    /**
     * Lists non-terminal stream lifecycle records in a namespace without opening data-plane
     * resources.
     *
     * <p>The result includes streams that are being created, active, or being deleted. Completed
     * deletion tombstones are not returned.
     *
     * @param namespaceName the namespace to inspect
     * @return a future resolving to metadata-only lifecycle entries
     */
    CompletableFuture<List<StreamCatalogEntry>> listStreamEntries(String namespaceName);

    /**
     * Creates a new stream.
     *
     * @param id the stream identifier
     * @param config stream configuration
     * @param partitioning partitioning configuration
     * @param schema schema configuration
     * @param properties user-defined properties
     * @return a future resolving to the created stream metadata
     * @throws io.lakestream.api.exception.AlreadyExistsException if the stream already exists
     * @throws io.lakestream.api.exception.StreamPermanentlyDeletedException if the identifier has
     *     a durable permanent-deletion fence and can never be created again
     * @throws UnsupportedOperationException if recovery encounters retired keyed allocations and
     *     the catalog storage cannot durably fence their mappings and writes
     */
    CompletableFuture<StreamMetadata> createStream(StreamIdentifier id, StreamConfig config,
                                                    Partitioning partitioning, SchemaConfig schema,
                                                    Map<String, String> properties);

    /**
     * Creates a new stream with an optional stream-level materialization policy.
     *
     * @param id the stream identifier
     * @param config stream configuration
     * @param partitioning partitioning configuration
     * @param schema schema configuration
     * @param properties user-defined properties
     * @param materialization optional stream-level materialization override
     * @return a future resolving to the created stream metadata
     * @throws io.lakestream.api.exception.AlreadyExistsException if the stream already exists
     * @throws io.lakestream.api.exception.StreamPermanentlyDeletedException if the identifier has
     *     a durable permanent-deletion fence and can never be created again
     * @throws UnsupportedOperationException if recovery encounters retired keyed allocations and
     *     the catalog storage cannot durably fence their mappings and writes
     */
    CompletableFuture<StreamMetadata> createStream(
            StreamIdentifier id, StreamConfig config, Partitioning partitioning,
            SchemaConfig schema, Map<String, String> properties,
            Optional<TableMaterializationPolicy> materialization);

    /**
     * Loads the full metadata for a stream.
     *
     * @param identifier the stream to load
     * @return a future resolving to the stream metadata
     * @throws io.lakestream.api.exception.NoSuchStreamException if not found
     */
    CompletableFuture<StreamMetadata> loadStream(StreamIdentifier identifier);

    /**
     * Idempotently increases the number of committed indexed partitions in a stream.
     *
     * <p>Existing partitions remain available while the new logs are provisioned. The committed
     * layout is published atomically only after every new partition has durable log and catalog
     * metadata.
     *
     * @param identifier the stream to expand
     * @param targetPartitionCount the desired total partition count
     * @return a future resolving to the resulting committed stream metadata
     */
    CompletableFuture<StreamMetadata> increasePartitions(
        StreamIdentifier identifier, int targetPartitionCount);

    /**
     * Replaces all stream properties using a monotonically increasing source revision.
     *
     * <p>A revision older than or equal to the last applied revision is an idempotent no-op.
     *
     * @param identifier the stream to update
     * @param properties the complete replacement property snapshot
     * @param sourceRevision the external source revision associated with the snapshot
     * @return a future resolving to the resulting stream metadata
     */
    CompletableFuture<StreamMetadata> replaceStreamProperties(
        StreamIdentifier identifier, Map<String, String> properties, long sourceRevision);

    /**
     * Resolves the effective materialization policy for a stream without opening data-plane
     * resources.
     *
     * @param identifier the stream to resolve
     * @return a future resolving to the materialization target, or empty when materialization is
     *     disabled or its referenced table catalog does not exist
     */
    CompletableFuture<Optional<ResolvedMaterialization>> resolveMaterialization(
        StreamIdentifier identifier);

    /**
     * Drops a stream.
     *
     * <p>Dropping an identifier with no live stream still durably tombstones that identifier so a
     * late create or reconciler cannot resurrect it. A later {@link #createStream} for the same
     * identifier must fail with a permanent-deletion error. Consequently, a {@code false} result
     * means that no live stream existed; it does not mean that catalog metadata was unchanged.
     *
     * @param identifier the stream to drop
     * @param purge if true, also purge all data; if false, only remove metadata
     * @return a future resolving to true if a live stream was dropped, false if no live stream
     *     existed before the permanent tombstone was installed
     * @throws UnsupportedOperationException if the catalog storage cannot durably fence keyed
     *     stream-ID mappings and writes to retired log IDs
     */
    CompletableFuture<Boolean> dropStream(StreamIdentifier identifier, boolean purge);

    /**
     * Checks whether a stream exists.
     *
     * @param identifier the stream to check
     * @return a future resolving to true if the stream exists
     */
    CompletableFuture<Boolean> streamExists(StreamIdentifier identifier);

    /**
     * Sets the stream-level materialization policy override.
     *
     * @param id the stream identifier
     * @param policy the override policy to apply
     * @return a future that completes when the override is set
     */
    CompletableFuture<Void> setStreamMaterialization(StreamIdentifier id, TableMaterializationPolicy policy);

    /**
     * Clears the stream-level materialization policy override.
     *
     * @param id the stream identifier
     * @return a future that completes when the override is cleared
     */
    CompletableFuture<Void> clearStreamMaterialization(StreamIdentifier id);

    // --- Data plane ---

    /**
     * Returns the layout describing how this stream is composed from logs.
     *
     * @param identifier the stream
     * @return a future resolving to the stream's layout
     */
    CompletableFuture<StreamLayout> getLayout(StreamIdentifier identifier);

    /**
     * Opens an existing log that belongs to the stream's committed layout.
     *
     * <p>This is a pure data-plane open operation. It must not allocate a log ID, register a
     * partition, grow the stream, or mutate catalog metadata.
     *
     * @param identifier the stream identifier
     * @param logId an existing log ID from the stream's committed layout
     * @return a future resolving to the opened log
     * @throws io.lakestream.api.exception.NoSuchStreamException if the stream is not active
     * @throws IllegalArgumentException if the log ID is not in the committed stream layout
     */
    CompletableFuture<Log> openLog(StreamIdentifier identifier, LogId logId);

    /**
     * Opens a writer for the stream.
     *
     * @param identifier the stream to write to
     * @return a future resolving to a stream writer
     */
    CompletableFuture<StreamWriter> openWriter(StreamIdentifier identifier);

    /**
     * Opens a reader for the stream.
     *
     * <p>The returned handle owns its lazily opened child logs and must be closed before the
     * catalog is closed.
     *
     * @param identifier the stream to read from
     * @return a future resolving to a stream reader
     */
    CompletableFuture<StreamReader> openReader(StreamIdentifier identifier);
}
