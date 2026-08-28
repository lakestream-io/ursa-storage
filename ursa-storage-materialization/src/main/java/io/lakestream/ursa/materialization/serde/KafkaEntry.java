/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/** Stable storage framing for one Kafka record: key length/key/value length/value. */
public record KafkaEntry(byte[] key, byte[] value) {

    public static KafkaEntry fromByteBuf(ByteBuf byteBuf) {
        // 1. Read key length
        if (byteBuf.readableBytes() < 4) {
            throw new IllegalArgumentException("Buffer too small to read key length");
        }
        int keyLen = byteBuf.readInt();
        if (keyLen < -1) {
            throw new IllegalArgumentException("Invalid Kafka key length: " + keyLen);
        }
        if (keyLen > byteBuf.readableBytes() - Integer.BYTES) {
            throw new IllegalArgumentException("Kafka key length exceeds the entry payload: " + keyLen);
        }

        // 2. Read key bytes
        byte[] key = null;
        if (keyLen >= 0) { // Handling -1 if you chose to support null-differentiation
            key = new byte[keyLen];
            if (keyLen > 0) {
                byteBuf.readBytes(key);
            }
        }

        // 3. Read value length
        if (byteBuf.readableBytes() < 4) {
            throw new IllegalArgumentException("Buffer too small to read value length");
        }
        int valLen = byteBuf.readInt();
        if (valLen < -1) {
            throw new IllegalArgumentException("Invalid Kafka value length: " + valLen);
        }
        if (valLen > byteBuf.readableBytes()) {
            throw new IllegalArgumentException("Kafka value length exceeds the entry payload: " + valLen);
        }

        // 4. Read value bytes
        byte[] value = null;
        if (valLen >= 0) {
            value = new byte[valLen];
            if (valLen > 0) {
                byteBuf.readBytes(value);
            }
        }
        if (byteBuf.isReadable()) {
            throw new IllegalArgumentException(
                    "Unexpected trailing bytes in Kafka entry: " + byteBuf.readableBytes());
        }

        return new KafkaEntry(key, value);
    }

    public ByteBuf toByteBuf() {
        int keyLen = (key == null) ? -1 : key.length;
        int valLen = (value == null) ? -1 : value.length;

        // 1. Precise allocation to avoid resizing
        int totalCapacity = 8 + Math.max(0, keyLen) + Math.max(0, valLen);

        // 2. Use a Pooled or Unpooled allocator depending on your framework
        // If using Netty, Unpooled.buffer(totalCapacity) is common
        ByteBuf buffer = Unpooled.buffer(totalCapacity);

        try {
            buffer.writeInt(keyLen);
            if (keyLen > 0) {
                buffer.writeBytes(key);
            }

            buffer.writeInt(valLen);
            if (valLen > 0) {
                buffer.writeBytes(value);
            }

            return buffer;
        } catch (Exception e) {
            // 3. Prevent memory leaks if writing fails
            buffer.release();
            throw e;
        }
    }
}
