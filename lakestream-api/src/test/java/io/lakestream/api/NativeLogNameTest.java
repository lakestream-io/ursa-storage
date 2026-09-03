/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** The one definition of the Lakestream-native log name: building it and reading it back. */
class NativeLogNameTest {

    @Test
    void shouldRoundTripASingleSegmentNamespace() {
        StreamIdentifier stream = StreamIdentifier.of("default", "orders-topic-id-DoZSD7MWQRGZSg7TTy1u7w");

        String logName = NativeLogName.of(stream, 3);

        assertThat(logName)
            .isEqualTo("lakestream-native/default/orders-topic-id-DoZSD7MWQRGZSg7TTy1u7w/partition-3");
        assertThat(NativeLogName.parse(logName)).isEqualTo(new NativeLogName.Parsed(stream, 3));
    }

    @Test
    void shouldRoundTripANamespaceContainingSlashes() {
        StreamIdentifier stream = StreamIdentifier.of("public/default", "orders");

        String logName = NativeLogName.of(stream, 0);

        assertThat(logName).isEqualTo("lakestream-native/public/default/orders/partition-0");
        assertThat(NativeLogName.parse(logName)).isEqualTo(new NativeLogName.Parsed(stream, 0));
    }

    @Test
    void shouldRoundTripANameThatItselfEndsInAPartitionSuffix() {
        StreamIdentifier stream = StreamIdentifier.of("default", "orders-partition-2");

        String logName = NativeLogName.of(stream, 5);

        assertThat(NativeLogName.parse(logName)).isEqualTo(new NativeLogName.Parsed(stream, 5));
    }

    @Test
    void shouldRecognizeOnlyItsOwnPrefix() {
        assertThat(NativeLogName.hasNativePrefix("lakestream-native/default/orders/partition-0")).isTrue();
        assertThat(NativeLogName.hasNativePrefix("default/orders-partition-0")).isFalse();
        assertThat(NativeLogName.hasNativePrefix(null)).isFalse();
    }

    @Test
    void shouldRejectAMissingPartitionSegment() {
        assertThatThrownBy(() -> NativeLogName.parse("lakestream-native/default/orders"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectATrailingSegmentThatIsNotAPartition() {
        assertThatThrownBy(() -> NativeLogName.parse("lakestream-native/default/orders/segment-0"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectAMissingNamespace() {
        assertThatThrownBy(() -> NativeLogName.parse("lakestream-native/orders/partition-0"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectANameThatIsNotNative() {
        assertThatThrownBy(() -> NativeLogName.parse("default/orders-partition-0"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectANegativePartition() {
        assertThatThrownBy(() -> NativeLogName.of(StreamIdentifier.of("default", "orders"), -1))
            .isInstanceOf(IllegalArgumentException.class);
    }
}
