/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.lakestream.api.StreamIdentifier;
import org.junit.jupiter.api.Test;

/** Verifies stream identity and partition parsing from canonical compaction-task log names. */
public class CompactionWorkerStreamNameTest {

    @Test
    public void testCanonicalAllocationKeyResolvesStreamIdentity() {
        String topic = "default/orders-topic-id-Eon_fZE2QTqu_NT0m6fD5Q-partition-3";

        assertEquals(StreamIdentifier.of("default", "orders-topic-id-Eon_fZE2QTqu_NT0m6fD5Q"),
                CompactionWorker.toStreamIdentifier(topic));
        assertEquals(3, CompactionWorker.partitionIndexOf(topic));
    }

    @Test
    public void testCanonicalAllocationKeyKeepsMultiSegmentNamespace() {
        String topic = "public/default/orders-partition-0";

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
    public void testMalformedNamespaceIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> CompactionWorker.toStreamIdentifier("sales//orders-partition-0"));
    }

    @Test
    public void testUriIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> CompactionWorker.toStreamIdentifier("s3://sales/orders-partition-0"));
    }
}
