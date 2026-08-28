/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.Position;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.AddResult;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.UrsaStorageTestBase;
import io.lakestream.ursa.storage.WalStorage;
import io.netty.buffer.Unpooled;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.internal.util.collections.Sets;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationRequest;
import software.amazon.awssdk.services.s3.model.LifecycleRule;

@Slf4j
public class AsyncCleanerTest {

    private static UrsaStorageTestBase ursaStorageTestBase;
    private static StorageApi storageApi;

    @BeforeEach
    public void setup() throws Exception {
        ursaStorageTestBase = new UrsaStorageTestBase();
        ursaStorageTestBase.setup(UrsaStorageTestBase.UrsaStorageTestConfig
            .builder()
            .ursaConfig(StorageConfig.builder()
                .writeBufferSize(1024 * 1024)
                .writeBufferFlushIntervalMs(10)
                .cleanupJobIntervalInHours(1)
                .idGeneratorType("dateuuid").build())
            .build());

        storageApi = ursaStorageTestBase.createStorageApi(InstrumentProvider.NOOP);
        storageApi.startWALCleanupService();
    }

    @AfterEach
    public void tearDown() {
        ursaStorageTestBase.cleanup();
    }

    @Test
    public void testPrefixes() {
        @Cleanup("stop")
        AsyncCleaner asyncCleaner = new AsyncCleaner();
        String from = "2024/01/01/01/01/01/1";
        String to = "2024/01/02/01/01/01/1";

        Set<String> prefixes = asyncCleaner.getPrefixes(from, to);
        assertEquals(24, prefixes.size());
    }

    @Test
    void testCleanup_FindsEarliestUncompactedPosition() throws Exception {
        // Arrange
        Set<Long> streams = new HashSet<>();
        streams.add(1L);
        streams.add(2L);
        streams.add(3L);

        StorageApi spyStorageApi = spy(storageApi);

        AsyncOxiaClient mockOxiaClient = spy(storageApi.getStorageOxiaClient());
        when(spyStorageApi.getStorageOxiaClient()).thenReturn(mockOxiaClient);

        when(spyStorageApi.listStreams()).thenReturn(CompletableFuture.completedFuture(streams));

        Position pos1 = new Position("2024/03/15/10/01/01/__dummy");
        Position pos2 = new Position("2024/03/15/09/01/01/__dummy");
        Position pos3 = new Position("2024/03/15/11/01/01/__dummy");

        when(spyStorageApi.getFirstUnCompactedPosition(1L)).thenReturn(CompletableFuture.completedFuture(pos1));
        when(spyStorageApi.getFirstUnCompactedPosition(2L)).thenReturn(CompletableFuture.completedFuture(pos2));
        when(spyStorageApi.getFirstUnCompactedPosition(3L)).thenReturn(CompletableFuture.completedFuture(pos3));

        WalStorage walStorage = spy(ursaStorageTestBase.getFailureInjectedStorage());

        FileStorage mockFileStorage = spy(walStorage.getFileStorage());
        when(walStorage.getFileStorage()).thenReturn(mockFileStorage);

        @Cleanup("stop")
        AsyncCleaner asyncCleaner = spy(new AsyncCleaner(spyStorageApi, walStorage,
            ursaStorageTestBase.getConfig().getUrsaConfig()));

        // Locking is covered by testLock. Avoid coupling this cleanup test to Oxia request timing under load.
        doNothing().when(asyncCleaner).lock();
        doNothing().when(asyncCleaner).unlock();
        when(asyncCleaner.getLastDeletedPosition()).thenReturn(CompletableFuture.completedFuture(
                "2024/03/15/06/01/01/01__dummy"));

        // Act
        asyncCleaner.cleanup();

        // Assert
        verify(mockFileStorage).deleteWithDatePrefixes(anySet());
        S3Client s3Client = ursaStorageTestBase.getS3BasedTestClass().s3Client;
        GetBucketLifecycleConfigurationRequest request = GetBucketLifecycleConfigurationRequest.builder()
            .bucket(ursaStorageTestBase.getS3BasedTestClass().bucket).build();
        List<LifecycleRule> rules = s3Client.getBucketLifecycleConfiguration(request).rules();
        assertEquals(3, rules.size());

        Set<String> expectedPrefixes = Sets.newSet(
            "2024/03/15/06/01/01",
            "2024/03/15/07/01/01",
            "2024/03/15/08/01/01"
        );

        for (LifecycleRule rule : rules) {
            String prefix = rule.filter().prefix();
            expectedPrefixes.remove(prefix);
        }

        assertTrue(expectedPrefixes.isEmpty());
        assertEquals("2024/03/15/09/01/01/__dummy", new String(mockOxiaClient.get("ursa-wal-delete-marker").get().value()));
    }

//    @Test
    public void simpleTest() throws Exception {
        @Cleanup("stop")
        AsyncCleaner asyncCleaner = new AsyncCleaner(storageApi, ursaStorageTestBase.getFailureInjectedStorage(),
            ursaStorageTestBase.getConfig().getUrsaConfig());

        long streamId = storageApi.generateStreamId().get();

        List<AddResult> entryHeaders = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            entryHeaders.add(storageApi.append(streamId, 1,
                Unpooled.wrappedBuffer(("test-" + i).getBytes())).get());
        }

        storageApi.softTrimStream(streamId, 3).get();

        asyncCleaner.cleanup();

        S3Client s3Client = ursaStorageTestBase.getS3BasedTestClass().s3Client;
        GetBucketLifecycleConfigurationRequest request = GetBucketLifecycleConfigurationRequest.builder()
            .bucket(ursaStorageTestBase.getS3BasedTestClass().bucket).build();
        List<LifecycleRule> rules = s3Client.getBucketLifecycleConfiguration(request).rules();
        for (LifecycleRule rule : rules) {
            log.debug("rule: {}", rule);
        }
    }

    @Test
    public void testLock() throws Exception {
        @Cleanup("stop")
        AsyncCleaner asyncCleaner = new AsyncCleaner(storageApi, ursaStorageTestBase.getFailureInjectedStorage(),
            ursaStorageTestBase.getConfig().getUrsaConfig());

        asyncCleaner.lock();

        try {
            asyncCleaner.lock();
            fail("Should fail to lock");
        } catch (Exception e) {
            // expected
        }

        asyncCleaner.unlock();

        try {
            asyncCleaner.lock();
        } catch (Exception e) {
            fail("Should be able to lock");
        }
    }
}
