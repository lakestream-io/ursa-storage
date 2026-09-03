/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * The catalog allocates Lakestream-native logs as {@code lakestream-native/<namespace>/<name>/partition-N},
 * which carries its partition as a trailing segment rather than as a suffix on the name. Table identity has to
 * resolve out of that shape as well as out of the canonical {@code namespace/name-partition-N} one.
 */
class TopicNameTest {

    @Test
    void shouldResolveIdentityOfNativeAllocationKey() {
        TopicName identity = TopicName.getStreamIdentity(
            "lakestream-native/default/orders-topic-id-DoZSD7MWQRGZSg7TTy1u7w/partition-0");

        assertThat(identity.getNamespace()).isEqualTo("default");
        assertThat(identity.getLocalName()).isEqualTo("orders-topic-id-DoZSD7MWQRGZSg7TTy1u7w");
    }

    @Test
    void shouldKeepMultiSegmentNamespaceOfNativeAllocationKey() {
        TopicName identity = TopicName.getStreamIdentity("lakestream-native/public/default/orders/partition-7");

        assertThat(identity.getNamespace()).isEqualTo("public/default");
        assertThat(identity.getLocalName()).isEqualTo("orders");
    }

    @Test
    void shouldResolveIdentityOfCanonicalName() {
        TopicName identity = TopicName.getStreamIdentity("public/default/orders-partition-12");

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
    void shouldRejectNativeKeyWithoutPartitionSegment() {
        assertThatThrownBy(() -> TopicName.getStreamIdentity("lakestream-native/default/orders"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldLeaveStoragePathParsingUnchangedForNativeKeys() {
        // Compacted objects are laid out under the allocation key as written, so plain parsing must not
        // start folding the native prefix away.
        TopicName parsed = TopicName.get("lakestream-native/default/orders/partition-0");

        assertThat(parsed.getNamespace()).isEqualTo("lakestream-native/default/orders");
        assertThat(parsed.getLocalName()).isEqualTo("partition-0");
    }
}
