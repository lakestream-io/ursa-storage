/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.kafka.schema;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.utils.ByteUtils;

/**
 * Framing of schema-registry-aware Kafka payloads.
 *
 * <p>Every payload starts with a magic byte ({@code 0}) followed by the big-endian 4-byte schema id.
 * Protobuf payloads additionally carry the <em>message indexes</em> that select the message type inside
 * the registered {@code .proto} file: a zig-zag varint count followed by that many zig-zag varint indexes,
 * where the single byte {@code 0} is shorthand for the index list {@code [0]}. The remainder of the
 * payload is the serialized record.
 */
public final class SchemaRegistryWireFormat {

    public static final byte MAGIC_BYTE = 0x0;
    public static final int SCHEMA_ID_SIZE = Integer.BYTES;
    public static final int HEADER_SIZE = 1 + SCHEMA_ID_SIZE;

    private static final List<Integer> FIRST_MESSAGE = List.of(0);

    private SchemaRegistryWireFormat() {
    }

    /**
     * Returns {@code true} when the payload starts with the magic byte and is long enough to hold a schema id.
     */
    public static boolean hasHeader(byte[] payload) {
        return payload != null && payload.length >= HEADER_SIZE && payload[0] == MAGIC_BYTE;
    }

    /**
     * Reads the magic byte and schema id, leaving the buffer positioned at the first byte after the header.
     *
     * @throws SerializationException when the header is truncated or the magic byte is unknown
     */
    public static int readSchemaId(ByteBuffer buffer) {
        if (buffer.remaining() < HEADER_SIZE) {
            throw new SerializationException("Payload is too short to carry a schema id: "
                    + buffer.remaining() + " bytes");
        }
        byte magic = buffer.get();
        if (magic != MAGIC_BYTE) {
            throw new SerializationException("Unknown magic byte " + magic);
        }
        return buffer.getInt();
    }

    /**
     * Reads the Protobuf message indexes, leaving the buffer positioned at the first byte of the record.
     *
     * @throws SerializationException when the index list is malformed
     */
    public static List<Integer> readMessageIndexes(ByteBuffer buffer) {
        try {
            int size = ByteUtils.readVarint(buffer);
            if (size == 0) {
                return FIRST_MESSAGE;
            }
            if (size < 0) {
                throw new SerializationException("Invalid message index count " + size);
            }
            List<Integer> indexes = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                int index = ByteUtils.readVarint(buffer);
                if (index < 0) {
                    throw new SerializationException("Invalid message index " + index);
                }
                indexes.add(index);
            }
            return Collections.unmodifiableList(indexes);
        } catch (IllegalArgumentException | BufferUnderflowException e) {
            throw new SerializationException("Malformed Protobuf message indexes", e);
        }
    }

    /** Encodes message indexes exactly as {@link #readMessageIndexes(ByteBuffer)} expects them. */
    public static byte[] writeMessageIndexes(List<Integer> indexes) {
        if (indexes.isEmpty()) {
            throw new IllegalArgumentException("Message indexes must not be empty");
        }
        if (indexes.size() == 1 && indexes.get(0) == 0) {
            return new byte[] {0};
        }
        int size = ByteUtils.sizeOfVarint(indexes.size());
        for (int index : indexes) {
            size += ByteUtils.sizeOfVarint(index);
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        ByteUtils.writeVarint(indexes.size(), buffer);
        for (int index : indexes) {
            ByteUtils.writeVarint(index, buffer);
        }
        return buffer.array();
    }

    /** Frames a serialized record: magic byte, schema id, optional message indexes, record bytes. */
    public static byte[] frame(int schemaId, byte[] messageIndexes, byte[] record) {
        int indexesLength = messageIndexes == null ? 0 : messageIndexes.length;
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + indexesLength + record.length);
        buffer.put(MAGIC_BYTE).putInt(schemaId);
        if (indexesLength > 0) {
            buffer.put(messageIndexes);
        }
        buffer.put(record);
        return buffer.array();
    }
}
