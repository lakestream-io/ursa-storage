/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static io.lakestream.ursa.storage.impl.StorageConfig.PROTOBUF_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.lakestream.api.FileInfo;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.WalStorageMetrics;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

public class TestReadCache {

    private static final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

    private static final StorageFormat format = new StorageFormat(StorageConfig.builder().build());

    private static final FileStorage fixedBufferFileStorage = new FileStorage() {
        @Override
        public CompletableFuture<Void> putAsync(ByteBuf data, String location) {
            return CompletableFuture.failedFuture(new Exception("no op"));
        }

        @Override
        public CompletableFuture<ByteBuf> getAsync(String location) {
            PersistCache cache = PersistCacheFactory.create(allocator, 10, PROTOBUF_VERSION);

            PendingAdd pendingAdd =
                    new PendingAdd(0, 1, Unpooled.wrappedBuffer(new byte[10]), new CompletableFuture<>(), null);
            cache.put(pendingAdd);
            return CompletableFuture.completedFuture(cache.serialize(location, format));
        }

        @Override
        public void delete(String location) throws IOException {

        }

        @Override
        public CompletableFuture<Void> deleteAsync(List<String> locations) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() throws Exception {

        }
    };

    @Test
    public void testReadCacheEvictBySize() throws Exception {
        StorageConfig config = new StorageConfig();
        config.setIndexSerializeFormatVersion(PROTOBUF_VERSION);
        config.setReadCacheMemorySize(1024);
        ReadCache readCache = new ReadCache(config, allocator, fixedBufferFileStorage, WalStorageMetrics.NULL);

        for (int i = 0; i < 100; i++) {
            var fi = new FileInfo("location" + i, 256);
            readCache.get(fi, 1).get();
        }
        Awaitility.await().untilAsserted(() -> {
            assertEquals(4, readCache.getReadCache().size());
        });
        readCache.close();

    }
}
