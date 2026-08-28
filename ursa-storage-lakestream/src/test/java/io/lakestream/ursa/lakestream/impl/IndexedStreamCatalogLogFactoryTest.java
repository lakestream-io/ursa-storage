/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.lakestream.api.CatalogPaths;
import io.lakestream.api.Log;
import io.lakestream.api.LogId;
import io.lakestream.api.LogStorage;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReader;
import io.lakestream.ursa.lakestream.reader.CompactedObjectReaderFactory;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class IndexedStreamCatalogLogFactoryTest {

    @Test
    void nameAwareCreatePassesNameAndOpenedReaderToLogFactory() {
        String name = "public/default/persistent/topic-partition-0";
        LogId logId = LogId.of(101L);
        CompactedObjectReaderFactory readerFactory = mock(CompactedObjectReaderFactory.class);
        CompactedObjectReader reader = mock(CompactedObjectReader.class);
        IndexedStreamCatalog.LogFactory logFactory = mock(IndexedStreamCatalog.LogFactory.class);
        Log log = mock(Log.class);
        when(readerFactory.open(name)).thenReturn(reader);
        when(logFactory.create(name, logId, reader)).thenReturn(log);
        IndexedStreamCatalog catalog = newCatalog(logFactory, readerFactory);

        assertSame(log, catalog.createLog(name, logId));

        verify(readerFactory).open(name);
        verify(logFactory).create(name, logId, reader);
        verify(reader, never()).close();
    }

    @Test
    void nameAwareCreateClosesOpenedReaderWhenLogFactoryFails() {
        String name = "public/default/persistent/topic-partition-1";
        LogId logId = LogId.of(102L);
        RuntimeException creationFailure = new RuntimeException("log creation failed");
        CompactedObjectReaderFactory readerFactory = mock(CompactedObjectReaderFactory.class);
        CompactedObjectReader reader = mock(CompactedObjectReader.class);
        IndexedStreamCatalog.LogFactory logFactory = mock(IndexedStreamCatalog.LogFactory.class);
        when(readerFactory.open(name)).thenReturn(reader);
        when(logFactory.create(name, logId, reader)).thenThrow(creationFailure);
        IndexedStreamCatalog catalog = newCatalog(logFactory, readerFactory);

        RuntimeException actual = assertThrows(RuntimeException.class, () -> catalog.createLog(name, logId));

        assertSame(creationFailure, actual);
        verify(reader).close();
    }

    @Test
    void nameAwareCreateDoesNotInvokeLogFactoryWhenReaderOpenFails() {
        String name = "public/default/persistent/topic-partition-2";
        LogId logId = LogId.of(103L);
        RuntimeException openFailure = new RuntimeException("reader open failed");
        CompactedObjectReaderFactory readerFactory = mock(CompactedObjectReaderFactory.class);
        IndexedStreamCatalog.LogFactory logFactory = mock(IndexedStreamCatalog.LogFactory.class);
        when(readerFactory.open(name)).thenThrow(openFailure);
        IndexedStreamCatalog catalog = newCatalog(logFactory, readerFactory);

        RuntimeException actual = assertThrows(RuntimeException.class, () -> catalog.createLog(name, logId));

        assertSame(openFailure, actual);
        verifyNoInteractions(logFactory);
    }

    @Test
    void nameAwareCreateRejectsNullReaderWithoutInvokingLogFactory() {
        String name = "public/default/persistent/topic-partition-3";
        LogId logId = LogId.of(104L);
        CompactedObjectReaderFactory readerFactory = mock(CompactedObjectReaderFactory.class);
        IndexedStreamCatalog.LogFactory logFactory = mock(IndexedStreamCatalog.LogFactory.class);
        when(readerFactory.open(name)).thenReturn(null);
        IndexedStreamCatalog catalog = newCatalog(logFactory, readerFactory);

        assertThrows(IllegalStateException.class, () -> catalog.createLog(name, logId));

        verifyNoInteractions(logFactory);
    }

    @Test
    void explicitReaderCreateTransfersReaderOwnershipToReturnedLog() {
        String name = "public/default/persistent/topic-partition-4";
        LogId logId = LogId.of(105L);
        CompactedObjectReader reader = mock(CompactedObjectReader.class);
        IndexedStreamCatalog.LogFactory logFactory = mock(IndexedStreamCatalog.LogFactory.class);
        CompactedObjectReaderFactory readerFactory = mock(CompactedObjectReaderFactory.class);
        Log log = mock(Log.class);
        when(logFactory.create(name, logId, reader)).thenReturn(log);
        IndexedStreamCatalog catalog = newCatalog(logFactory, readerFactory);

        assertSame(log, catalog.createLog(name, logId, reader));

        verify(logFactory).create(name, logId, reader);
        verify(reader, never()).close();
        verifyNoInteractions(readerFactory);
    }

    @Test
    void explicitReaderCreateClosesReaderWhenLogFactoryFails() {
        String name = "public/default/persistent/topic-partition-5";
        LogId logId = LogId.of(106L);
        RuntimeException creationFailure = new RuntimeException("log creation failed");
        CompactedObjectReader reader = mock(CompactedObjectReader.class);
        IndexedStreamCatalog.LogFactory logFactory = mock(IndexedStreamCatalog.LogFactory.class);
        when(logFactory.create(name, logId, reader)).thenThrow(creationFailure);
        IndexedStreamCatalog catalog = newCatalog(logFactory, mock(CompactedObjectReaderFactory.class));

        RuntimeException actual = assertThrows(RuntimeException.class,
            () -> catalog.createLog(name, logId, reader));

        assertSame(creationFailure, actual);
        verify(reader).close();
    }

    @Test
    void explicitReaderCreateClosesReaderWhenLogFactoryReturnsNull() {
        String name = "public/default/persistent/topic-partition-6";
        LogId logId = LogId.of(107L);
        CompactedObjectReader reader = mock(CompactedObjectReader.class);
        IndexedStreamCatalog.LogFactory logFactory = mock(IndexedStreamCatalog.LogFactory.class);
        when(logFactory.create(name, logId, reader)).thenReturn(null);
        IndexedStreamCatalog catalog = newCatalog(logFactory, mock(CompactedObjectReaderFactory.class));

        assertThrows(IllegalStateException.class, () -> catalog.createLog(name, logId, reader));

        verify(reader).close();
    }

    @Test
    void explicitReaderCreateRejectsNullBeforeInvokingLogFactory() {
        IndexedStreamCatalog.LogFactory logFactory = mock(IndexedStreamCatalog.LogFactory.class);
        IndexedStreamCatalog catalog = newCatalog(logFactory, mock(CompactedObjectReaderFactory.class));

        assertThrows(NullPointerException.class,
            () -> catalog.createLog("public/default/persistent/topic-partition-7", LogId.of(108L), null));

        verifyNoInteractions(logFactory);
    }

    @Test
    void explicitReaderCreateSuppressesReaderCloseFailure() {
        String name = "public/default/persistent/topic-partition-8";
        LogId logId = LogId.of(109L);
        RuntimeException creationFailure = new RuntimeException("log creation failed");
        RuntimeException closeFailure = new RuntimeException("reader close failed");
        CompactedObjectReader reader = mock(CompactedObjectReader.class);
        IndexedStreamCatalog.LogFactory logFactory = mock(IndexedStreamCatalog.LogFactory.class);
        when(logFactory.create(name, logId, reader)).thenThrow(creationFailure);
        doThrow(closeFailure).when(reader).close();
        IndexedStreamCatalog catalog = newCatalog(logFactory, mock(CompactedObjectReaderFactory.class));

        RuntimeException actual = assertThrows(RuntimeException.class,
            () -> catalog.createLog(name, logId, reader));

        assertSame(creationFailure, actual);
        assertEquals(1, actual.getSuppressed().length);
        assertSame(closeFailure, actual.getSuppressed()[0]);
    }

    @Test
    void explicitReaderCreatePreservesFailureWhenReaderCloseRethrowsIt() {
        String name = "public/default/persistent/topic-partition-8-self-suppression";
        LogId logId = LogId.of(110L);
        RuntimeException creationFailure = new RuntimeException("log creation failed");
        CompactedObjectReader reader = mock(CompactedObjectReader.class);
        IndexedStreamCatalog.LogFactory logFactory = mock(IndexedStreamCatalog.LogFactory.class);
        when(logFactory.create(name, logId, reader)).thenThrow(creationFailure);
        doThrow(creationFailure).when(reader).close();
        IndexedStreamCatalog catalog = newCatalog(logFactory, mock(CompactedObjectReaderFactory.class));

        RuntimeException actual = assertThrows(RuntimeException.class,
            () -> catalog.createLog(name, logId, reader));

        assertSame(creationFailure, actual);
        assertEquals(0, actual.getSuppressed().length);
    }

    @Test
    void idOnlyCreatePreservesRawFactoryCompatibility() {
        LogId logId = LogId.of(111L);
        Log rawLog = mock(Log.class);
        CompactedObjectReaderFactory readerFactory = mock(CompactedObjectReaderFactory.class);
        IndexedStreamCatalog catalog = new IndexedStreamCatalog(
            mock(AsyncOxiaClient.class),
            mock(CatalogPaths.class),
            mock(LogStorage.class),
            ignored -> rawLog,
            null,
            ignored -> CompletableFuture.completedFuture(1L),
            readerFactory,
            null,
            List.of());

        assertSame(rawLog, catalog.createLog(logId));

        verifyNoInteractions(readerFactory);
    }

    @Test
    void legacyFactoryRejectsNameAwareCreateBeforeOpeningReader() {
        String name = "public/default/persistent/topic-partition-9";
        Log rawLog = mock(Log.class);
        CompactedObjectReaderFactory readerFactory = mock(CompactedObjectReaderFactory.class);
        IndexedStreamCatalog catalog = new IndexedStreamCatalog(
            mock(AsyncOxiaClient.class),
            mock(CatalogPaths.class),
            mock(LogStorage.class),
            ignored -> rawLog,
            null,
            ignored -> CompletableFuture.completedFuture(1L),
            readerFactory,
            null,
            List.of());

        assertThrows(UnsupportedOperationException.class,
            () -> catalog.createLog(name, LogId.of(112L)));

        verifyNoInteractions(readerFactory);
    }

    @Test
    void legacyFactoryRejectsExplicitReaderCreateAndClosesTransferredReader() {
        String name = "public/default/persistent/topic-partition-10";
        Log rawLog = mock(Log.class);
        CompactedObjectReader reader = mock(CompactedObjectReader.class);
        CompactedObjectReaderFactory readerFactory = mock(CompactedObjectReaderFactory.class);
        IndexedStreamCatalog catalog = new IndexedStreamCatalog(
            mock(AsyncOxiaClient.class),
            mock(CatalogPaths.class),
            mock(LogStorage.class),
            ignored -> rawLog,
            null,
            ignored -> CompletableFuture.completedFuture(1L),
            readerFactory,
            null,
            List.of());

        assertThrows(UnsupportedOperationException.class,
            () -> catalog.createLog(name, LogId.of(113L), reader));

        verifyNoInteractions(readerFactory);
        verify(reader).close();
    }

    private static IndexedStreamCatalog newCatalog(
            IndexedStreamCatalog.LogFactory logFactory,
            CompactedObjectReaderFactory readerFactory) {
        return new IndexedStreamCatalog(
            mock(AsyncOxiaClient.class),
            mock(CatalogPaths.class),
            mock(LogStorage.class),
            logFactory,
            null,
            ignored -> CompletableFuture.completedFuture(1L),
            readerFactory,
            null,
            List.of());
    }
}
