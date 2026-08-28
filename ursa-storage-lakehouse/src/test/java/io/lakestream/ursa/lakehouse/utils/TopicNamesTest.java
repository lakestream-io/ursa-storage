/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class TopicNamesTest {

    @Test
    void shouldCanonicalizeAndStripPartition() {
        String name = "public/default/orders-partition-12";

        assertThat(TopicNames.canonical(name)).isEqualTo("public/default/orders-partition-12");
        assertThat(TopicNames.partitionedTopicName(name)).isEqualTo("public/default/orders");
        assertThat(TopicNames.partitionedLocalName(name)).isEqualTo("orders");
        assertThat(TopicNames.storagePath("s3a://bucket/root", name))
                .isEqualTo("s3a://bucket/root/public/default/orders");
    }

    @Test
    void shouldNormalizeShortAndCanonicalNames() {
        assertThat(TopicNames.canonical("orders")).isEqualTo("default/orders");
        assertThat(TopicNames.canonical("sales/orders")).isEqualTo("sales/orders");
        assertThat(TopicNames.canonical("public/default/orders"))
                .isEqualTo("public/default/orders");
    }

    @Test
    void shouldRejectInvalidNames() {
        assertThatThrownBy(() -> TopicNames.canonical("sales//orders"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TopicNames.canonical("sales/"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
