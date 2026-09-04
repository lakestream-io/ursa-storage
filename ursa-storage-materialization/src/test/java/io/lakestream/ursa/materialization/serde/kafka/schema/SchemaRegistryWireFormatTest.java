/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.kafka.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

class SchemaRegistryWireFormatTest {

    @Test
    void readsMagicByteAndSchemaId() {
        byte[] payload = SchemaRegistryWireFormat.frame(0x01020304, null, "{}".getBytes(StandardCharsets.UTF_8));
        assertThat(payload).startsWith(0, 1, 2, 3, 4);
        assertThat(SchemaRegistryWireFormat.hasHeader(payload)).isTrue();

        ByteBuffer buffer = ByteBuffer.wrap(payload);
        assertThat(SchemaRegistryWireFormat.readSchemaId(buffer)).isEqualTo(0x01020304);
        assertThat(buffer.position()).isEqualTo(SchemaRegistryWireFormat.HEADER_SIZE);
        assertThat(buffer.remaining()).isEqualTo(2);
    }

    @Test
    void rejectsPayloadsWithoutHeader() {
        assertThat(SchemaRegistryWireFormat.hasHeader(null)).isFalse();
        assertThat(SchemaRegistryWireFormat.hasHeader(new byte[] {0, 0, 0})).isFalse();
        assertThat(SchemaRegistryWireFormat.hasHeader(new byte[] {1, 0, 0, 0, 7})).isFalse();

        assertThatThrownBy(() -> SchemaRegistryWireFormat.readSchemaId(ByteBuffer.wrap(new byte[] {0, 0, 0})))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("too short");
        assertThatThrownBy(() -> SchemaRegistryWireFormat.readSchemaId(ByteBuffer.wrap(new byte[] {1, 0, 0, 0, 7})))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("magic byte");
    }

    @Test
    void encodesMessageIndexesLikeTheSchemaRegistrySerializers() {
        // Single first-message index is the one-byte shorthand; other lists are zig-zag varints.
        assertThat(SchemaRegistryWireFormat.writeMessageIndexes(List.of(0))).containsExactly(0);
        assertThat(SchemaRegistryWireFormat.writeMessageIndexes(List.of(1, 2))).containsExactly(4, 2, 4);
        assertThat(SchemaRegistryWireFormat.writeMessageIndexes(List.of(300))).containsExactly(2, -40, 4);
        assertThat(SchemaRegistryWireFormat.writeMessageIndexes(List.of(0, 0))).containsExactly(4, 0, 0);
    }

    @Test
    void roundTripsMessageIndexes() {
        for (List<Integer> indexes : List.of(List.of(0), List.of(1), List.of(0, 0), List.of(1, 2), List.of(300, 7, 1))) {
            byte[] record = {9, 9, 9};
            byte[] payload = SchemaRegistryWireFormat.frame(42,
                    SchemaRegistryWireFormat.writeMessageIndexes(indexes), record);
            ByteBuffer buffer = ByteBuffer.wrap(payload);
            assertThat(SchemaRegistryWireFormat.readSchemaId(buffer)).isEqualTo(42);
            assertThat(SchemaRegistryWireFormat.readMessageIndexes(buffer)).isEqualTo(indexes);
            assertThat(buffer.remaining()).isEqualTo(record.length);
        }
    }

    @Test
    void rejectsMalformedMessageIndexes() {
        // A negative count (zig-zag 1 == -1).
        assertThatThrownBy(() -> SchemaRegistryWireFormat.readMessageIndexes(ByteBuffer.wrap(new byte[] {1})))
                .isInstanceOf(SerializationException.class);
        // Count of two but only one index present.
        assertThatThrownBy(() -> SchemaRegistryWireFormat.readMessageIndexes(ByteBuffer.wrap(new byte[] {4, 2})))
                .isInstanceOf(SerializationException.class);
        // Truncated varint.
        assertThatThrownBy(() -> SchemaRegistryWireFormat.readMessageIndexes(ByteBuffer.wrap(new byte[] {(byte) 0x80})))
                .isInstanceOf(SerializationException.class);
        assertThatThrownBy(() -> SchemaRegistryWireFormat.writeMessageIndexes(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
