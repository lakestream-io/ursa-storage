/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static io.lakestream.ursa.storage.impl.StorageConfig.PROTOBUF_VERSION;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class PersistCacheFactory  {

    public static PersistCache create(ByteBufAllocator allocator, int maxCacheSize, int version) {
        if (version < PROTOBUF_VERSION) {
            return new EntryCacheV0(allocator, maxCacheSize);
        } else {
            return new EntryCache(allocator, maxCacheSize);
        }
    }

    public static PersistCache deserialize(ByteBufAllocator allocator, ByteBuf data, int version) {
        if (version < PROTOBUF_VERSION) {
            return EntryCacheV0.deserialize(allocator, data);
        } else {
            return EntryCache.deserialize(allocator, data);
        }
    }
}
