/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static io.lakestream.ursa.storage.impl.StorageConfig.DEFAULT_INDEX_SERIALIZE_FORMAT_VERSION;

import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.FileBasedTestClass;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPersistWriteCache extends FileBasedTestClass {
    @Test
    public void testSerDe() throws Exception {
        StorageConfig config = new StorageConfig();
        config.setStoragePath(path.toAbsolutePath().toString());
        @Cleanup
        LocalFileStorage storage = new LocalFileStorage(config, InstrumentProvider.NOOP);
        ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
        @Cleanup
        PersistCache cache =
                PersistCacheFactory.create(allocator, 1 * 1024 * 1024, config.getIndexSerializeFormatVersion());

        final long lId = 1;
        HashMap<Integer, ByteBuf> dataMap = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            byte[] data = "test".getBytes();
            ByteBuf buf = allocator.buffer(data.length).writeBytes(data);
            PendingAdd pendingAdd = new PendingAdd(lId, 1, buf, new CompletableFuture<>(), null);
            cache.put(pendingAdd);
            dataMap.put(i, buf);
        }

        String dataPath = UUID.randomUUID().toString();
        cache.persist(storage, dataPath, new StorageFormat(config)).join();

        ByteBuf buf = storage.get(dataPath);
        @Cleanup
        PersistCache cache2 = PersistCacheFactory.deserialize(allocator, buf,
                config.getIndexSerializeFormatVersion());
        Assertions.assertEquals(10, cache2.entryCount());
        for (int entryId = 0; entryId < 10; entryId++) {
            byte[] expected = new byte[dataMap.get(entryId).readableBytes()];
            dataMap.get(entryId).readBytes(expected);
            final var data = cache2.get(lId, entryId);
            Assertions.assertNotNull(data);
            byte[] actual = new byte[data.readableBytes()];
            data.readBytes(actual);
            Assertions.assertArrayEquals(expected, actual);
        }
        dataMap.forEach((k, v) -> {
            v.release();
        });
    }

    @Test
    public void testRefCount() throws Exception {
        ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
        StorageConfig config = new StorageConfig();
        config.setStoragePath(path.toString());
        @Cleanup
        LocalFileStorage storage = new LocalFileStorage(config, InstrumentProvider.NOOP);
        @Cleanup
        PersistCache cache =
                PersistCacheFactory.create(allocator, 1 * 1024 * 1024, DEFAULT_INDEX_SERIALIZE_FORMAT_VERSION);

        final long lId = 1;
        byte[] data = "test".getBytes();
        ByteBuf buf = allocator.buffer(data.length).writeBytes(data);
        for (int i = 0; i < 10; i++) {
            buf.retain();
            PendingAdd pendingAdd = new PendingAdd(lId, 1, buf, new CompletableFuture<>(), null);
            cache.put(pendingAdd);
        }

        Assertions.assertEquals(21, buf.refCnt());

        UUID uuid = UUID.randomUUID();
        cache.persist(storage, String.valueOf(uuid), new StorageFormat(config));

        Assertions.assertEquals(21, buf.refCnt());
        cache.clear();
        Assertions.assertEquals(21, buf.refCnt());
        buf.release(21);
    }

}
