/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

class KafkaSourceMetadataTest {

    @Test
    void explicitLogicalTopicIsAuthoritative() {
        assertThat(KafkaSourceMetadata.topicName(
                        "default/orders-topic-id-RQttf5YgR5-xrS63xWM3FA-partition-3",
                        Map.of(KafkaSourceMetadata.TOPIC_NAME_PROPERTY, "orders-partition-3")))
                .isEqualTo("orders-partition-3");
    }

    @Test
    void sourceLogicalNameTakesPrecedenceOverLegacyKafkaProperty() {
        assertThat(KafkaSourceMetadata.topicName(
                        "default/orders-topic-id-RQttf5YgR5-xrS63xWM3FA-partition-3",
                        Map.of(
                                KafkaSourceMetadata.LOGICAL_NAME_PROPERTY, "orders",
                                KafkaSourceMetadata.TOPIC_NAME_PROPERTY, "legacy-orders")))
                .isEqualTo("orders");
    }

    @Test
    void directStreamFallbackRemovesNamespaceAndPartitionSuffix() {
        assertThat(KafkaSourceMetadata.topicName("default/orders-partition-3", Map.of()))
                .isEqualTo("orders");
    }

    @Test
    void nonPartitionSuffixIsPreserved() {
        assertThat(KafkaSourceMetadata.topicName("default/orders-partition-blue", Map.of()))
                .isEqualTo("orders-partition-blue");
    }
}
