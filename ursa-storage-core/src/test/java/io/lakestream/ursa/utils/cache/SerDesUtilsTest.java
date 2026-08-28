/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.utils.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import lombok.Cleanup;
import org.junit.jupiter.api.Test;


public class SerDesUtilsTest {

    @Test
    void testVarIntWriteAndRead() {
        ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
        int[] tests = new int[]{
                Integer.MIN_VALUE, -123456, -12345, -1234, -123, -12, 0, 1, 12, 123, 1234, 12345, 123456,
                Integer.MAX_VALUE};
        @Cleanup("release")
        var b = allocator.buffer();
        for (var n : tests) {
            SerDesUtils.writeVarInt(b, n);
        }

        for (var n : tests) {
            assertEquals(n, SerDesUtils.readVarInt(b));
        }
    }
}
