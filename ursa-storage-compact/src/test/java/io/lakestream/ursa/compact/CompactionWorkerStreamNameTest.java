/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.lakestream.api.StreamIdentifier;
import org.junit.jupiter.api.Test;

/**
 * Compaction tasks carry the log name the catalog allocated. For Lakestream-native streams that name is
 * {@code lakestream-native/<namespace>/<name>/partition-N}, so the materialization dispatch has to resolve
 * the stream identity out of that shape as well as out of the shorter forms.
 */
public class CompactionWorkerStreamNameTest {

    @Test
    public void testNativeAllocationKeyResolvesStreamIdentity() {
        String topic = "lakestream-native/default/orders-topic-id-Eon_fZE2QTqu_NT0m6fD5Q/partition-3";

        assertEquals(StreamIdentifier.of("default", "orders-topic-id-Eon_fZE2QTqu_NT0m6fD5Q"),
                CompactionWorker.toStreamIdentifier(topic));
        assertEquals(3, CompactionWorker.partitionIndexOf(topic));
    }

    @Test
    public void testNativeAllocationKeyKeepsMultiSegmentNamespace() {
        String topic = "lakestream-native/public/default/orders/partition-0";

        assertEquals(StreamIdentifier.of("public/default", "orders"),
                CompactionWorker.toStreamIdentifier(topic));
        assertEquals(0, CompactionWorker.partitionIndexOf(topic));
    }

    @Test
    public void testNamespacedNameWithPartitionSuffix() {
        String topic = "sales/orders-partition-2";

        assertEquals(StreamIdentifier.of("sales", "orders"), CompactionWorker.toStreamIdentifier(topic));
        assertEquals(2, CompactionWorker.partitionIndexOf(topic));
    }

    @Test
    public void testBareNameDefaultsNamespaceAndPartition() {
        String topic = "orders";

        assertEquals(StreamIdentifier.of("default", "orders"), CompactionWorker.toStreamIdentifier(topic));
        assertEquals(0, CompactionWorker.partitionIndexOf(topic));
    }

    @Test
    public void testUnrecognizedMultiSegmentNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> CompactionWorker.toStreamIdentifier("sales/2026/orders"));
    }

    @Test
    public void testNativePrefixWithoutPartitionSegmentIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> CompactionWorker.toStreamIdentifier("lakestream-native/default/orders"));
    }
}
