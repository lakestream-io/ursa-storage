/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.util;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;

public class ByteBufUtils {
    public static byte[] getByteArray(ByteBuf buffer) {
        if (buffer.hasArray()) {
            return buffer.array();
        }

        final byte[] bytes = new byte[buffer.readableBytes()];
        buffer.getBytes(buffer.readerIndex(), bytes);
        return bytes;
    }

    public static ByteBuffer getNioBuffer(ByteBuf buffer) {
        if (buffer.isDirect()) {
            return buffer.nioBuffer();
        }
        final byte[] bytes = new byte[buffer.readableBytes()];
        buffer.getBytes(buffer.readerIndex(), bytes);
        return ByteBuffer.wrap(bytes);
    }

    public static long bytesToLong(byte[] b) {
        if (b == null || b.length < 8) {
            throw new IllegalArgumentException("The input byte array in illegal.");
        }
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.put(b);
        buffer.flip();
        return buffer.getLong();
    }

    public static byte[] longToBytes(long data) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(data);
        return buffer.array();
    }
}
