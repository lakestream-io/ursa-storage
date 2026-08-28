/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.lakestream.api.StreamIdentifier;
import org.junit.jupiter.api.Test;

class DefaultCatalogPathsTest {

    private final DefaultCatalogPaths paths = new DefaultCatalogPaths("/streams", "/admin/streams");

    @Test
    void testStreamMetadataPath() {
        StreamIdentifier id = new StreamIdentifier("default", "my-topic");
        assertEquals("/streams/default/my-topic", paths.streamMetadataPath(id));
    }

    @Test
    void testStreamConfigPath() {
        StreamIdentifier id = new StreamIdentifier("default", "my-topic");
        assertEquals("/admin/streams/default/my-topic", paths.streamConfigPath(id));
    }

    @Test
    void testPartitionMetadataPath() {
        StreamIdentifier id = new StreamIdentifier("default", "my-topic");
        assertEquals("/streams/default/my-topic-partition-0",
            paths.partitionMetadataPath(id, 0));
    }

    @Test
    void testNamespacePrefix() {
        assertEquals("/streams/default/", paths.namespacePrefix("default"));
    }

    @Test
    void testPartitionPrefix() {
        StreamIdentifier id = new StreamIdentifier("default", "my-topic");
        assertEquals("/streams/default/my-topic-partition-", paths.partitionPrefix(id));
    }

    @Test
    void testNamespacePath() {
        assertEquals("/admin/streams/_namespaces/my-ns", paths.namespacePath("my-ns"));
    }

    @Test
    void testNamespacesPrefix() {
        assertEquals("/admin/streams/_namespaces/", paths.namespacesPrefix());
    }

    @Test
    void testStreamConfigPrefix() {
        assertEquals("/admin/streams/default/", paths.streamConfigPrefix("default"));
    }

    @Test
    void testTableCatalogPath() {
        assertEquals("/admin/streams/_tablecatalogs/iceberg-prod",
            paths.tableCatalogPath("iceberg-prod"));
    }

    @Test
    void testTableCatalogsPrefix() {
        assertEquals("/admin/streams/_tablecatalogs/", paths.tableCatalogsPrefix());
    }
}
