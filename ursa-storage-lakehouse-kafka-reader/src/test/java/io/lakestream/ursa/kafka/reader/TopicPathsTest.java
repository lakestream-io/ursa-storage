/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.kafka.reader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class TopicPathsTest {

    @Test
    void normalizesLogNameAndRemovesPartitionSuffixFromStoragePath() {
        String logName = TopicPaths.canonicalLogName("sales/orders-partition-7");

        assertThat(logName).isEqualTo("sales/orders-partition-7");
        assertThat(TopicPaths.storagePath("s3a://bucket/root/", logName))
                .isEqualTo("s3a://bucket/root/sales/orders");
    }

    @Test
    void usesDefaultNamespaceForUnqualifiedLogName() {
        assertThat(TopicPaths.canonicalLogName("orders-partition-0"))
                .isEqualTo("default/orders-partition-0");
    }

    @Test
    void rejectsUriStyleLogNames() {
        assertThatThrownBy(() -> TopicPaths.canonicalLogName("legacy://sales/orders"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
