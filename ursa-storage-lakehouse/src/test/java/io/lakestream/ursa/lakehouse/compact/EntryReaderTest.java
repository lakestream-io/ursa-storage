/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;

import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.exception.DataSourceException;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.lakehouse.exception.LakehouseException;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.AddResult;
import io.lakestream.ursa.storage.Entry;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.UrsaStorageTestBase;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
@Slf4j
public class EntryReaderTest {

    private static UrsaStorageTestBase testBase;
    private static StorageApi storageApi;

    @BeforeAll
    static void setup() throws Exception {
        var config = StorageConfig.builder().backendStorageType("local").build();
        testBase = new UrsaStorageTestBase();
        testBase.setup(
            UrsaStorageTestBase.UrsaStorageTestConfig.builder()
                .ursaConfig(config)
                .build()
        );
        storageApi = testBase.createStorageApi(InstrumentProvider.NOOP);
    }

    @AfterAll
    static void clean() throws IOException {
        storageApi.close();
        testBase.cleanup();
    }

    @Test
    public void testFillEntries() throws LakehouseException, InterruptedException {
        int streamId = 1;

        List<CompletableFuture<AddResult>> futures = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            futures.add(storageApi.append(streamId, 10, Unpooled.wrappedBuffer(("test-" + i).getBytes())));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();


        try (var reader = new EntryReader(storageApi, streamId, 0, 100, 1000, CompactionMetrics.NOOP)) {
            Entry entry;
            long previousOffset = -1;
            long readCount = 0;
            while ((entry = reader.readEntry()) != null) {
                try {
                    readCount++;
                    if (entry.header().offset() <= previousOffset) {
                        fail("The offset should be in order");
                    }
                    previousOffset = entry.header().offset();
                } finally {
                    entry.payload().release();
                }
            }
            assertEquals(10, readCount);
        }
    }

    @Test
    public void testFillEntriesWithTruncatePartCase() throws LakehouseException, InterruptedException,
        ExecutionException {
        int streamId = 1;

        for (int i = 0; i < 30; i++) {
            storageApi.append(streamId, 10, Unpooled.wrappedBuffer(("test-" + i).getBytes())).get();
        }

        storageApi.softTrimStream(streamId, 50).get();

        try (var reader = new EntryReader(storageApi, streamId, 0, 100, 1000, CompactionMetrics.NOOP)) {
            Entry entry;
            long previousOffset = -1;
            long readCount = 0;
            while ((entry = reader.readEntry()) != null) {
                try {
                    readCount++;
                    if (entry.header().offset() <= previousOffset) {
                        fail("The offset should be in order");
                    }
                    previousOffset = entry.header().offset();
                } finally {
                    entry.payload().release();
                }
            }
            assertEquals(5, readCount);
        }
    }

    @Test
    public void testFillEntriesWithTruncateAllCase() throws LakehouseException, InterruptedException,
        ExecutionException {
        int streamId = 1;

        for (int i = 0; i < 30; i++) {
            storageApi.append(streamId, 10, Unpooled.wrappedBuffer(("test-" + i).getBytes())).get();
        }

        storageApi.softTrimStream(streamId, 100).get();

        try (var reader = new EntryReader(storageApi, streamId, 0, 100, 1000, CompactionMetrics.NOOP)) {
            try {
                reader.readEntry();
            } catch (Exception e) {
                assertInstanceOf(LakehouseException.class, e);
                assertEquals("No such entries during the range 1:[0-100]",
                    e.getCause().getMessage());
            }
        }
    }

    @Test
    public void testFillEntriesWithTruncateAllCase1() throws LakehouseException, InterruptedException,
        ExecutionException {
        int streamId = 1;

        for (int i = 0; i < 30; i++) {
            storageApi.append(streamId, 10, Unpooled.wrappedBuffer(("test-" + i).getBytes())).get();
        }

        storageApi.softTrimStream(streamId, 100).get();

        try (var reader = new EntryReader(storageApi, streamId, 0, 100, 1000, CompactionMetrics.NOOP)) {
            try {
                reader.read();
            } catch (Exception e) {
                assertInstanceOf(DataSourceException.class, e);
                assertEquals(((DataSourceException) e).getExceptionCode().getCode(),
                    ExceptionCode.NO_SUCH_ENTRIES.getCode());
            }
        }
    }

    @Test
    public void testFillEntriesWithHardDeleteAllCase() throws Exception {
        int streamId = 2;

        for (int i = 0; i < 30; i++) {
            storageApi.append(streamId, 10, Unpooled.wrappedBuffer(("test-" + i).getBytes())).get();
        }

        storageApi.hardTrimStream(streamId, 300).get();

        try (var reader = new EntryReader(storageApi, streamId, 0, 100, 1000, CompactionMetrics.NOOP)) {
            try {
                reader.read();
                fail();
            } catch (Exception e) {
                assertInstanceOf(DataSourceException.class, e);
                assertEquals(ExceptionCode.NO_SUCH_OFFSET.getCode(),
                    ((DataSourceException) e).getExceptionCode().getCode());
            }
        }
    }

}
