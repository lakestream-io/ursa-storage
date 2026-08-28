/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.api.LogStateManager;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.EntryList;
import io.lakestream.ursa.storage.WalStorage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class PersistStorageApiFailureCleanupTest {

    @Test
    void clearsEntriesPopulatedBeforeWalReadFailure() {
        WalStorage walStorage = mock(WalStorage.class);
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        LogStateManager logStateManager = mock(LogStateManager.class);
        StorageConfig config = StorageConfig.builder().backendStorageType("local").build();
        PersistStorageApi storageApi = new PersistStorageApi(
                config,
                oxiaClient,
                walStorage,
                InstrumentProvider.NOOP,
                new StorageFormat(config),
                logStateManager);
        EntryList entryList = new EntryList(1L);
        ByteBuf payload = Unpooled.buffer().writeByte(1);
        IllegalStateException readFailure = new IllegalStateException("wal read failed");

        when(walStorage.get(anyList(), same(entryList))).thenAnswer(invocation -> {
            entryList.add(Entry.of(new EntryHeader(0L, 1, 0L, 1, 1L), payload));
            return CompletableFuture.failedFuture(readFailure);
        });

        CompletionException thrown = assertThrows(
                CompletionException.class,
                () -> storageApi.readEntries(List.<EntryIndex>of(), entryList).join());

        assertSame(readFailure, thrown.getCause());
        assertEquals(0, payload.refCnt());
        assertTrue(entryList.isEmpty());
        assertEquals(0, readFailure.getSuppressed().length);
    }
}
