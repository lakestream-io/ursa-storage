/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Level 2 interface: stream catalog for metadata management.
 *
 * <p>Provides CRUD operations for namespaces and streams, plus factory methods
 * for obtaining {@link StreamLayout}, {@link StreamWriter}, and {@link StreamReader}.
 *
 * <p>Implementations:
 * <ul>
 *   <li>{@code IndexedStreamCatalog} — Oxia-backed catalog using indexed partitions</li>
 *   <li>{@code LakestreamCatalog} — rill-compatible catalog with new metadata format</li>
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
     * {@link Stream#effectiveMaterialization()} resolves a stream policy first, then its namespace
     * policy, and finally this cluster default. It lets {@code materializationEnabled=true} (with no
     * {@code materializationDefaultNamespace}) materialize every stream in every namespace without
     * per-namespace authoring.
     *
     * @param policy the cluster-wide default policy
     * @return a future that completes when the policy is set
     */
    default CompletableFuture<Void> setClusterDefaultMaterialization(TableMaterializationPolicy policy) {
        throw new UnsupportedOperationException(
                "cluster-default materialization is not supported by this catalog");
    }

    /**
     * Returns the cluster-wide default materialization policy, or {@link Optional#empty()} if none is
     * set. Used as the lowest-priority fallback when resolving {@link Stream#effectiveMaterialization()}.
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
     * Creates a new stream.
     *
     * @param id the stream identifier
     * @param config stream configuration
     * @param partitioning partitioning configuration
     * @param schema schema configuration
     * @param properties user-defined properties
     * @return a future resolving to the created stream metadata
     * @throws io.lakestream.api.exception.AlreadyExistsException if the stream already exists
     */
    CompletableFuture<Stream> createStream(StreamIdentifier id, StreamConfig config,
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
     */
    CompletableFuture<Stream> createStream(StreamIdentifier id, StreamConfig config,
                                            Partitioning partitioning, SchemaConfig schema,
                                            Map<String, String> properties,
                                            Optional<TableMaterializationPolicy> materialization);

    /**
     * Loads the full metadata for a stream.
     *
     * @param identifier the stream to load
     * @return a future resolving to the stream metadata
     * @throws io.lakestream.api.exception.NoSuchStreamException if not found
     */
    CompletableFuture<Stream> loadStream(StreamIdentifier identifier);

    /**
     * Idempotently registers a single partition of a stream whose underlying log was created
     * outside this catalog (for example, a stream created by an external control plane rather than through
     * {@link #createStream}). Writes the catalog partition metadata for {@code partitionIndex} using
     * the supplied {@code streamId}, then grows the stream config so its partition count is at least
     * {@code partitionIndex + 1}, preserving any existing properties and materialization policy.
     *
     * <p>This lets a consumer (for example, the materialization compaction worker) resolve an
     * broker-created stream via {@link #loadStream} that would otherwise throw
     * {@link io.lakestream.api.exception.NoSuchStreamException}. Safe to call
     * concurrently and repeatedly.
     *
     * @param id             the partition-stripped stream identity
     * @param partitionIndex the partition being registered (0 for a non-partitioned stream)
     * @param streamId       the underlying log id for this partition
     * @param properties     stream properties to seed when first creating the config (may be empty)
     */
    CompletableFuture<Void> registerExternalPartition(StreamIdentifier id, int partitionIndex,
                                                      long streamId, Map<String, String> properties);

    /**
     * Opens a partition whose lifecycle is controlled by an external system.
     *
     * <p>The implementation derives the persistent log name, creates or reuses its keyed log ID,
     * registers the partition in the catalog, and returns a reader-aware log handle.
     *
     * @param id the partition-stripped stream identity
     * @param partitionIndex the zero-based partition index
     * @param properties stream properties to seed when first registering the stream
     * @return a future resolving to the opened log
     */
    CompletableFuture<Log> openExternalPartition(StreamIdentifier id, int partitionIndex,
                                                  Map<String, String> properties);

    /**
     * Deletes an externally controlled partition.
     *
     * <p>Deletion is ordered so log data is removed before catalog metadata, and catalog metadata
     * is removed before the keyed log-ID mapping. The operation also cleans up a keyed mapping left
     * behind by historical writes whose catalog partition metadata is already absent.
     *
     * @param id the partition-stripped stream identity
     * @param partitionIndex the zero-based partition index
     * @return a future that completes when deletion is finished
     */
    CompletableFuture<Void> deleteExternalPartition(StreamIdentifier id, int partitionIndex);

    /**
     * Drops a stream.
     *
     * @param identifier the stream to drop
     * @param purge if true, also purge all data; if false, only remove metadata
     * @return a future resolving to true if the stream was dropped, false if it didn't exist
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
     * Sets properties on a stream (merge semantics).
     *
     * @param id the stream identifier
     * @param props the properties to set
     * @return a future that completes when the properties are updated
     */
    CompletableFuture<Void> setStreamProperties(StreamIdentifier id, Map<String, String> props);

    /**
     * Removes properties from a stream.
     *
     * @param id the stream identifier
     * @param keys the property keys to remove
     * @return a future that completes when the properties are removed
     */
    CompletableFuture<Void> removeStreamProperties(StreamIdentifier id, List<String> keys);

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

    // --- Lifecycle ---

    /**
     * Seals a stream — no more writes accepted, reads still work.
     *
     * @param identifier the stream to seal
     * @return a future that completes when the stream is sealed
     */
    CompletableFuture<Void> sealStream(StreamIdentifier identifier);

    /**
     * Truncates all data in a stream.
     *
     * @param identifier the stream to truncate
     * @return a future that completes when truncation is done
     */
    CompletableFuture<Void> truncateStream(StreamIdentifier identifier);

    // --- Data plane ---

    /**
     * Returns the layout describing how this stream is composed from logs.
     *
     * @param identifier the stream
     * @return a future resolving to the stream's layout
     */
    CompletableFuture<StreamLayout> getLayout(StreamIdentifier identifier);

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
     * @param identifier the stream to read from
     * @return a future resolving to a stream reader
     */
    CompletableFuture<StreamReader> openReader(StreamIdentifier identifier);
}
