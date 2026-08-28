/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lakestream.api.EntryHeader;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.EntryList;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class ObjectWalStorageCleanupTest {

    @Test
    void releasesSuccessfulStorageReadWhenAnotherReadFails() {
        ByteBuf successfulPayload = Unpooled.buffer().writeByte(1);
        IllegalStateException readFailure = new IllegalStateException("storage read failed");

        ObjectWalStorageImpl.releaseCompletedBuffers(
                List.of(
                        CompletableFuture.completedFuture(successfulPayload),
                        CompletableFuture.failedFuture(readFailure)),
                0,
                readFailure);

        assertEquals(0, successfulPayload.refCnt());
        assertEquals(0, readFailure.getSuppressed().length);
    }

    @Test
    void separatesInstalledAndUninstalledStorageReadOwnership() {
        ByteBuf installedPayload = Unpooled.buffer().writeByte(1);
        ByteBuf uninstalledPayload = Unpooled.buffer().writeByte(2);
        EntryList entryList = new EntryList(1L);
        entryList.add(Entry.of(new EntryHeader(0L, 1, 0L, 1, 1L), installedPayload));
        IllegalStateException mappingFailure = new IllegalStateException("mapping failed");

        ObjectWalStorageImpl.releaseCompletedBuffers(
                List.of(
                        CompletableFuture.completedFuture(installedPayload),
                        CompletableFuture.completedFuture(uninstalledPayload)),
                1,
                mappingFailure);

        assertEquals(1, installedPayload.refCnt());
        assertEquals(0, uninstalledPayload.refCnt());

        ObjectWalStorageImpl.clearEntryListAfterFailure(entryList, mappingFailure);

        assertEquals(0, installedPayload.refCnt());
        assertTrue(entryList.isEmpty());
        assertEquals(0, mappingFailure.getSuppressed().length);
    }
}
