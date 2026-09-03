/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** Verifies stream identity parsing from canonical compaction-task log names. */
class TopicNameTest {

    @Test
    void shouldResolveIdentityOfCanonicalAllocationKey() {
        TopicName identity = TopicName.getStreamIdentity(
            "default/orders-topic-id-DoZSD7MWQRGZSg7TTy1u7w-partition-0");

        assertThat(identity.getNamespace()).isEqualTo("default");
        assertThat(identity.getLocalName()).isEqualTo("orders-topic-id-DoZSD7MWQRGZSg7TTy1u7w");
    }

    @Test
    void shouldKeepMultiSegmentNamespace() {
        TopicName identity = TopicName.getStreamIdentity("public/default/orders-partition-7");

        assertThat(identity.getNamespace()).isEqualTo("public/default");
        assertThat(identity.getLocalName()).isEqualTo("orders");
    }

    @Test
    void shouldResolveIdentityOfBareName() {
        TopicName identity = TopicName.getStreamIdentity("orders");

        assertThat(identity.getNamespace()).isEqualTo(TopicName.DEFAULT_NAMESPACE);
        assertThat(identity.getLocalName()).isEqualTo("orders");
    }

    @Test
    void shouldRejectAUri() {
        assertThatThrownBy(() -> TopicName.getStreamIdentity("s3://default/orders-partition-0"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldPreserveThePartitionInPlainParsing() {
        TopicName parsed = TopicName.get("public/default/orders-partition-7");

        assertThat(parsed.getNamespace()).isEqualTo("public/default");
        assertThat(parsed.getLocalName()).isEqualTo("orders-partition-7");
    }
}
