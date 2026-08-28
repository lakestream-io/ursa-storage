/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static io.lakestream.ursa.storage.impl.StorageConfig.JVM_MAX_DIRECT_MEMORY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.IDGenerator;
import io.netty.buffer.PooledByteBufAllocator;
import lombok.Cleanup;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDefaultConfiguration {

    StorageConfig config = new StorageConfig();

    @Test
    public void testDefaultConfigurationOfSimpleStorageImpl() throws Exception {
        IDGenerator generator = IDGenerator.create("memory", null, null);
        @Cleanup
        ObjectWalStorageImpl simpleStorage = new ObjectWalStorageImpl(PooledByteBufAllocator.DEFAULT,
            mock(FileStorage.class),
            generator, config, InstrumentProvider.NOOP, null, new StorageFormat(config), new StreamStateManagerImpl());
        simpleStorage.initialize();
        int expectedPendingAddRequests = Math.toIntExact(Math.round(JVM_MAX_DIRECT_MEMORY * 0.15));
        Assertions.assertEquals(config.getMaxPendingAddRequestsUsedBytes(), expectedPendingAddRequests);
    }

    @Test
    public void testGetWriteBufferSegment() {
        // Test default calculation
        StorageConfig config = StorageConfig.builder()
                .writeBufferSize(4 * 1024 * 1024) // 4MB (default)
                .build();

        // Expected: (JVM_MAX_DIRECT_MEMORY * 0.25) / writeBufferSize
        long expectedDefault = Math.toIntExact(
            Math.round(StorageConfig.JVM_MAX_DIRECT_MEMORY * 0.25 / (4 * 1024 * 1024)));
        assertEquals(expectedDefault, config.getWriteBufferSegment());

        // Test calculation with 1MB buffer
        config = StorageConfig.builder()
                .writeBufferSize(1024 * 1024) // 1MB
                .build();

        long expectedWith1MB = Math.toIntExact(
            Math.round(StorageConfig.JVM_MAX_DIRECT_MEMORY * 0.25 / (1024 * 1024)));
        assertEquals(expectedWith1MB, config.getWriteBufferSegment());

        // Test explicit setting
        int explicitValue = 42;
        config = StorageConfig.builder()
                .writeBufferSegment(explicitValue)
                .build();
        assertEquals(explicitValue, config.getWriteBufferSegment());
    }
}
