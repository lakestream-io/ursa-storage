/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import io.lakestream.api.CatalogPaths;
import io.lakestream.api.StreamIdentifier;

/**
 * Configurable catalog paths with custom prefixes.
 *
 * <p>The no-argument constructor uses the standard Lakestream keyspace:
 * <pre>
 *   var paths = new DefaultCatalogPaths("/streams", "/admin/streams");
 *   // streamMetadataPath → /streams/default/my-topic
 *   // streamConfigPath   → /admin/streams/default/my-topic
 * </pre>
 */
public class DefaultCatalogPaths implements CatalogPaths {

    private static final String DEFAULT_METADATA_PREFIX = "/streams";
    private static final String DEFAULT_CONFIG_PREFIX = "/admin/streams";

    private final String metadataPrefix;
    private final String configPrefix;

    /** Creates catalog paths in the standard Lakestream keyspace. */
    public DefaultCatalogPaths() {
        this(DEFAULT_METADATA_PREFIX, DEFAULT_CONFIG_PREFIX);
    }

    /**
     * Creates catalog paths with custom prefixes.
     *
     * @param metadataPrefix prefix for stream metadata (e.g., "/streams")
     * @param configPrefix prefix for stream config (e.g., "/admin/streams")
     */
    public DefaultCatalogPaths(String metadataPrefix, String configPrefix) {
        this.metadataPrefix = metadataPrefix;
        this.configPrefix = configPrefix;
    }

    @Override
    public String streamMetadataPath(StreamIdentifier id) {
        return metadataPrefix + "/" + id.namespace() + "/" + id.name();
    }

    @Override
    public String streamConfigPath(StreamIdentifier id) {
        return configPrefix + "/" + id.namespace() + "/" + id.name();
    }

    @Override
    public String streamTombstonePath(StreamIdentifier id) {
        return configPrefix + "/_tombstones/" + id.namespace() + "/" + id.name();
    }

    @Override
    public String partitionMetadataPath(StreamIdentifier id, int partitionIndex) {
        return metadataPrefix + "/" + id.namespace() + "/" + id.name()
            + "-partition-" + partitionIndex;
    }

    @Override
    public String namespacePrefix(String namespace) {
        return metadataPrefix + "/" + namespace + "/";
    }

    @Override
    public String partitionPrefix(StreamIdentifier id) {
        return metadataPrefix + "/" + id.namespace() + "/" + id.name() + "-partition-";
    }

    @Override
    public String namespacePath(String namespace) {
        return configPrefix + "/_namespaces/" + namespace;
    }

    @Override
    public String namespacesPrefix() {
        return configPrefix + "/_namespaces/";
    }

    @Override
    public String streamConfigPrefix(String namespace) {
        return configPrefix + "/" + namespace + "/";
    }

    @Override
    public String tableCatalogPath(String name) {
        return configPrefix + "/_tablecatalogs/" + name;
    }

    @Override
    public String tableCatalogsPrefix() {
        return configPrefix + "/_tablecatalogs/";
    }
}
