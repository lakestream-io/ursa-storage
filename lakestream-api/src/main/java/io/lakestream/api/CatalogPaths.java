/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

/**
 * Strategy interface for constructing metadata key paths in the catalog store.
 *
 * <p>Catalog deployments can use different key prefixes for storing stream metadata in Oxia.
 * This interface keeps path construction separate from catalog behavior.
 *
 * <p>Reserved namespace segments: {@code _tombstones} holds permanent-deletion tombstones, just as
 * {@code _tablecatalogs} holds registered table catalogs and {@code _namespaces} holds namespace
 * records. A stream namespace must never be named after a reserved segment, or its streams would
 * collide with those records.
 */
public interface CatalogPaths {

    /** Reserved namespace segment holding permanent-deletion tombstones. */
    String TOMBSTONE_SEGMENT = "_tombstones";

    /**
     * Path for stream metadata.
     *
     * @param id the stream identifier
     * @return the metadata path (e.g., "/streams/default/my-stream")
     */
    String streamMetadataPath(StreamIdentifier id);

    /**
     * Path for stream config (e.g., partition count).
     *
     * @param id the stream identifier
     * @return the config path (e.g., "/admin/streams/default/my-stream")
     */
    String streamConfigPath(StreamIdentifier id);

    /**
     * Path for a specific partition's metadata.
     *
     * @param id the stream identifier
     * @param partitionIndex the zero-based partition index
     * @return the partition metadata path
     */
    String partitionMetadataPath(StreamIdentifier id, int partitionIndex);

    /**
     * Prefix for scanning all streams in a namespace.
     *
     * @param namespace the namespace name
     * @return the namespace scan prefix
     */
    String namespacePrefix(String namespace);

    /**
     * Prefix for scanning all partitions of a stream.
     *
     * @param id the stream identifier
     * @return the partition scan prefix
     */
    String partitionPrefix(StreamIdentifier id);

    /**
     * Path for namespace metadata.
     *
     * @param namespace the namespace name
     * @return the namespace metadata path
     */
    String namespacePath(String namespace);

    /**
     * Prefix for scanning all namespaces.
     *
     * @return the namespaces scan prefix
     */
    String namespacesPrefix();

    /**
     * Prefix for scanning all stream config entries in a namespace.
     *
     * @param namespace the namespace name
     * @return the stream config scan prefix (e.g., "/admin/streams/default/")
     */
    String streamConfigPrefix(String namespace);

    /**
     * Prefix under which every permanent-deletion tombstone is stored.
     *
     * <p>The default places the reserved {@link #TOMBSTONE_SEGMENT} segment where a namespace would
     * go, which keeps tombstones outside every real namespace's {@link #streamConfigPrefix(String)}
     * while sharing the same root (e.g., "/admin/streams/_tombstones/").
     *
     * @return the tombstone scan prefix
     */
    default String streamTombstonePrefix() {
        return streamConfigPrefix(TOMBSTONE_SEGMENT);
    }

    /**
     * Key of the permanent-deletion tombstone for a stream identity.
     *
     * <p>Tombstones live outside {@link #streamConfigPrefix(String)} so listing a namespace never
     * reads completed deletions. The default appends the identity below
     * {@link #streamTombstonePrefix()}; the namespace is joined here rather than passed into
     * {@code streamConfigPrefix}, which takes a single namespace name and not a multi-segment path.
     *
     * @param id the stream identifier
     * @return the tombstone path (e.g., "/admin/streams/_tombstones/default/my-stream")
     */
    default String streamTombstonePath(StreamIdentifier id) {
        return streamTombstonePrefix() + id.namespace() + "/" + id.name();
    }

    /**
     * Name passed to the compacted-object reader factory for one stream log.
     *
     * <p>The default format is suitable for generic indexed streams. Protocol-specific path
     * strategies may override it when their external reader uses a different canonical name.
     *
     * @param id the stream identifier
     * @param logIndex the zero-based log index within the stream layout
     * @return the external reader name for this log
     */
    default String compactedReaderName(StreamIdentifier id, int logIndex) {
        return id.fullName() + "-partition-" + logIndex;
    }

    /**
     * Path for a registered table catalog record.
     *
     * <p>Table catalogs live in a sibling keyspace to namespaces and streams.
     * Implementations follow the same prefix pattern that other admin records
     * use (e.g., {@code /admin/tablecatalogs/<name>}).
     *
     * @param name the table catalog name
     * @return the metadata path for the named table catalog
     */
    String tableCatalogPath(String name);

    /**
     * Prefix for scanning all registered table catalogs.
     *
     * @return the table catalog scan prefix
     */
    String tableCatalogsPrefix();
}
