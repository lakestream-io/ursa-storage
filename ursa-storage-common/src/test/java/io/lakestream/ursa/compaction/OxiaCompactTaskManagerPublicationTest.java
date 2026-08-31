/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.ursa.compaction.task.CompactOffsetSerde;
import io.lakestream.ursa.compaction.task.CompactedOffset;
import io.lakestream.ursa.compaction.task.PreparedCompactStreamTask;
import io.lakestream.ursa.compaction.task.PreparedCompactStreamTaskSerde;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class OxiaCompactTaskManagerPublicationTest {

    private static final String TOPIC = "org/analytics/orders";
    private static final long STREAM_ID = 42L;

    @Test
    void leaseUsesEphemeralCreateAndConditionalRelease() throws Exception {
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        when(client.put(anyString(), any(), anySet())).thenAnswer(invocation ->
                CompletableFuture.completedFuture(new PutResult(invocation.getArgument(0), version(7L))));
        when(client.delete(anyString(), anySet())).thenReturn(CompletableFuture.completedFuture(true));
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);

        CompactTaskManager.PublicationLease lease =
                manager.tryAcquirePublicationLease(TOPIC, STREAM_ID).orElseThrow();
        assertTrue(manager.releasePublicationLease(lease));

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        verify(client).put(key.capture(), any(), eq(Set.of(
                PutOption.PartitionKey(TOPIC),
                PutOption.IfRecordDoesNotExist,
                PutOption.AsEphemeralRecord)));
        verify(client).delete(eq(key.getValue()), eq(Set.of(
                DeleteOption.PartitionKey(TOPIC),
                DeleteOption.IfVersionIdEquals(7L))));
    }

    @Test
    void cancellingAsyncReleaseCancelsTheUnderlyingOxiaDelete() {
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        CompletableFuture<Boolean> oxiaDelete = new CompletableFuture<>();
        when(client.delete(anyString(), anySet())).thenReturn(oxiaDelete);
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);
        CompactTaskManager.PublicationLease lease =
                new CompactTaskManager.PublicationLease(TOPIC, STREAM_ID, "owner", 7L);

        CompletableFuture<Boolean> release = manager.releasePublicationLeaseAsync(lease);
        assertTrue(release.cancel(false));

        assertTrue(oxiaDelete.isCancelled());
    }

    @Test
    void leaseHasOneWinnerAndStaleReleaseCannotDeleteSuccessor() throws Exception {
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        when(client.put(anyString(), any(), anySet())).thenReturn(CompletableFuture.failedFuture(
                new KeyAlreadyExistsException("publication-lease")));
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);

        assertTrue(manager.tryAcquirePublicationLease(TOPIC, STREAM_ID).isEmpty());

        CompactTaskManager.PublicationLease stale =
                new CompactTaskManager.PublicationLease(TOPIC, STREAM_ID, "old-owner", 7L);
        when(client.delete(anyString(), anySet())).thenReturn(CompletableFuture.failedFuture(
                new UnexpectedVersionIdException("publication-lease", 8L)));
        assertFalse(manager.releasePublicationLease(stale));

        verify(client).delete(startsWith("publication-lease-"), eq(Set.of(
                DeleteOption.PartitionKey(TOPIC), DeleteOption.IfVersionIdEquals(7L))));
    }

    @Test
    void cursorClaimConflictPermanentlyFencesWithoutRefreshingRevision() throws Exception {
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        CompactTaskManager.PublicationLease lease =
                new CompactTaskManager.PublicationLease(TOPIC, STREAM_ID, "owner", 7L);
        byte[] leaseValue = (STREAM_ID + "\n" + lease.ownerId()).getBytes(StandardCharsets.UTF_8);
        GetResult leaseResult = new GetResult("lease", leaseValue, version(lease.revision()));
        String cursorKey = "compact-offset-" + TOPIC;
        CompactedOffset cursor = new CompactedOffset(STREAM_ID, 99L, 100L);
        GetResult cursorResult = new GetResult(cursorKey,
                CompactOffsetSerde.INSTANCE.serialize(cursor), version(11L));
        when(client.get(anyString(), anySet())).thenAnswer(invocation -> {
            String key = invocation.getArgument(0);
            return CompletableFuture.completedFuture(key.startsWith("publication-lease-")
                    ? leaseResult : cursorResult);
        });
        when(client.put(eq(cursorKey), any(), anySet())).thenReturn(CompletableFuture.failedFuture(
                new UnexpectedVersionIdException(cursorKey, 12L)));
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);

        assertThrows(PublicationFencedException.class,
                () -> manager.claimPublishedOffset(lease));

        verify(client, times(2)).get(startsWith("publication-lease-"),
                eq(Set.of(GetOption.PartitionKey(TOPIC))));
        verify(client, times(1)).get(eq(cursorKey), eq(Set.of(GetOption.PartitionKey(TOPIC))));
        verify(client, times(1)).put(eq(cursorKey), any(), eq(Set.of(
                PutOption.PartitionKey(TOPIC), PutOption.IfVersionIdEquals(11L))));
    }

    @Test
    void cursorAdvancePersistsCumulativeSize() throws Exception {
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        CompactTaskManager.PublicationLease lease =
                new CompactTaskManager.PublicationLease(TOPIC, STREAM_ID, "owner", 7L);
        byte[] leaseValue = (STREAM_ID + "\n" + lease.ownerId()).getBytes(StandardCharsets.UTF_8);
        when(client.get(anyString(), anySet())).thenReturn(CompletableFuture.completedFuture(
                new GetResult("lease", leaseValue, version(lease.revision()))));
        String cursorKey = "compact-offset-" + TOPIC;
        when(client.put(eq(cursorKey), any(), anySet())).thenReturn(CompletableFuture.completedFuture(
                new PutResult(cursorKey, version(12L))));
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);
        CompactTaskManager.PublishedOffsetClaim current = new CompactTaskManager.PublishedOffsetClaim(
                new CompactedOffset(STREAM_ID, 9L, 100L), 11L);
        CompactedOffset updated = new CompactedOffset(STREAM_ID, 19L, 250L);

        CompactTaskManager.PublishedOffsetClaim result =
                manager.compareAndSetPublishedOffset(lease, current, updated);

        ArgumentCaptor<byte[]> serialized = ArgumentCaptor.forClass(byte[].class);
        verify(client).put(eq(cursorKey), serialized.capture(), eq(Set.of(
                PutOption.PartitionKey(TOPIC), PutOption.IfVersionIdEquals(11L))));
        assertEquals(updated, CompactOffsetSerde.INSTANCE.deserialize(serialized.getValue()));
        assertEquals(250L, result.offset().getCumulativeSize());
    }

    @Test
    void namedOffsetUpdatePersistsCumulativeSize() throws Exception {
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        String cursorKey = "compact-offset-" + TOPIC;
        when(client.put(eq(cursorKey), any(), anySet())).thenReturn(CompletableFuture.completedFuture(
                new PutResult(cursorKey, version(12L))));
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);

        manager.updatePublishedOffset(TOPIC, STREAM_ID, 19L, 250L);

        ArgumentCaptor<byte[]> serialized = ArgumentCaptor.forClass(byte[].class);
        verify(client).put(eq(cursorKey), serialized.capture(), eq(Set.of(PutOption.PartitionKey(TOPIC))));
        assertEquals(
                new CompactedOffset(STREAM_ID, 19L, 250L),
                CompactOffsetSerde.INSTANCE.deserialize(serialized.getValue()));
    }

    @Test
    void repairsLegacyCursorBeforePreparedTaskCasWithoutNarrowingCumulativeSize()
            throws Exception {
        long taskCumulativeSize = (long) Integer.MAX_VALUE + 1_000L;
        long taskTotalSize = 250L;
        CompactedOffset legacy = new CompactedOffset(STREAM_ID, 9L, 0L);
        PreparedCompactStreamTask prepared = preparedTask(
                10L, 20L, taskTotalSize, taskCumulativeSize);
        AsyncOxiaClient client = repairClient(legacy, prepared);
        CompactTaskManager.PublicationLease lease = lease();
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);

        assertTrue(manager.repairLegacyPublishedOffset(lease));

        ArgumentCaptor<byte[]> serialized = ArgumentCaptor.forClass(byte[].class);
        verify(client).put(eq("compact-offset-" + TOPIC), serialized.capture(), eq(Set.of(
                PutOption.PartitionKey(TOPIC), PutOption.IfVersionIdEquals(11L))));
        CompactedOffset repaired = CompactOffsetSerde.INSTANCE.deserialize(serialized.getValue());
        assertEquals(taskCumulativeSize - taskTotalSize, repaired.getCumulativeSize());
        assertTrue(repaired.getCumulativeSize() > Integer.MAX_VALUE);
        assertEquals(legacy.getOffset(), repaired.getOffset());
    }

    @Test
    void repairsLegacyCursorAfterPreparedTaskCas() throws Exception {
        long taskCumulativeSize = (long) Integer.MAX_VALUE + 1_000L;
        CompactedOffset legacy = new CompactedOffset(STREAM_ID, 19L, 0L);
        PreparedCompactStreamTask prepared = preparedTask(
                10L, 20L, 250L, taskCumulativeSize);
        prepared.setStatus(PreparedCompactStreamTask.PUSHED_TASK);
        AsyncOxiaClient client = repairClient(legacy, prepared);
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);

        assertTrue(manager.repairLegacyPublishedOffset(lease()));

        ArgumentCaptor<byte[]> serialized = ArgumentCaptor.forClass(byte[].class);
        verify(client).put(eq("compact-offset-" + TOPIC), serialized.capture(), eq(Set.of(
                PutOption.PartitionKey(TOPIC), PutOption.IfVersionIdEquals(11L))));
        CompactedOffset repaired = CompactOffsetSerde.INSTANCE.deserialize(serialized.getValue());
        assertEquals(taskCumulativeSize, repaired.getCumulativeSize());
        assertEquals(legacy.getOffset(), repaired.getOffset());
    }

    @Test
    void rejectsLegacyCursorWhenNoPreparedTaskCanProveItsCumulativeSize() throws Exception {
        CompactedOffset legacy = new CompactedOffset(STREAM_ID, 9L, 0L);
        AsyncOxiaClient client = repairClient(legacy, null);
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);

        LegacyPublishedOffsetException error = assertThrows(
                LegacyPublishedOffsetException.class,
                () -> manager.repairLegacyPublishedOffset(lease()));

        assertEquals(TOPIC, error.publicationName());
        assertEquals(STREAM_ID, error.streamId());
        assertEquals(9L, error.offset());
        assertTrue(error.getMessage().contains("no durable prepared task exists"));
        verify(client, never()).put(eq("compact-offset-" + TOPIC), any(), anySet());
    }

    @Test
    void claimLoudlyRejectsUnrepairedLegacyCursor() throws Exception {
        CompactedOffset legacy = new CompactedOffset(STREAM_ID, 9L, 0L);
        AsyncOxiaClient client = repairClient(legacy, null);
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);

        LegacyPublishedOffsetException error = assertThrows(
                LegacyPublishedOffsetException.class,
                () -> manager.claimPublishedOffset(lease()));

        assertTrue(error.getMessage().contains("no safe automatic repair was completed"));
        verify(client, never()).put(eq("compact-offset-" + TOPIC), any(), anySet());
    }

    @Test
    void claimRejectsNonzeroCumulativeSizeForEmptyCursor() throws Exception {
        CompactedOffset invalid = new CompactedOffset(STREAM_ID, -1L, 1L);
        AsyncOxiaClient client = repairClient(invalid, null);
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);

        PublicationRecoveryException error = assertThrows(
                PublicationRecoveryException.class,
                () -> manager.claimPublishedOffset(lease()));

        assertTrue(error.getMessage().contains("Stored published-offset cursor"));
        verify(client, never()).put(eq("compact-offset-" + TOPIC), any(), anySet());
    }

    @Test
    void rootJsonNullCursorIsRecoveryFailureAcrossReadsRepairAndClaim() throws Exception {
        byte[] rootJsonNull = "null".getBytes(StandardCharsets.UTF_8);
        AsyncOxiaClient client = publicationMetadataClient(rootJsonNull, null);
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);

        assertThrows(PublicationRecoveryException.class,
                () -> manager.getPublishedOffset(STREAM_ID));
        assertThrows(PublicationRecoveryException.class,
                () -> manager.getPublishedOffset(TOPIC));
        assertThrows(PublicationRecoveryException.class,
                () -> manager.repairLegacyPublishedOffset(lease()));
        assertThrows(PublicationRecoveryException.class,
                () -> manager.claimPublishedOffset(lease()));

        verify(client, never()).put(eq("compact-offset-" + TOPIC), any(), anySet());
    }

    @Test
    void runtimeCursorDecodeFailureIsRecoveryFailureAndCannotOverwriteCursor() throws Exception {
        AsyncOxiaClient client = publicationMetadataClient(null, null);
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);

        PublicationRecoveryException error = assertThrows(
                PublicationRecoveryException.class,
                () -> manager.claimPublishedOffset(lease()));

        assertTrue(error.getCause() instanceof NullPointerException);
        verify(client, never()).put(eq("compact-offset-" + TOPIC), any(), anySet());
    }

    @Test
    void claimStillResetsValidCursorFromDifferentStreamIncarnation() throws Exception {
        CompactedOffset previous = new CompactedOffset(STREAM_ID + 1L, 99L, 1_000L);
        AsyncOxiaClient client = publicationMetadataClient(
                CompactOffsetSerde.INSTANCE.serialize(previous), null);
        String cursorKey = "compact-offset-" + TOPIC;
        when(client.put(eq(cursorKey), any(), anySet())).thenReturn(
                CompletableFuture.completedFuture(new PutResult(cursorKey, version(12L))));
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);

        CompactTaskManager.PublishedOffsetClaim claim = manager.claimPublishedOffset(lease());

        assertEquals(new CompactedOffset(STREAM_ID, -1L, 0L), claim.offset());
        ArgumentCaptor<byte[]> serialized = ArgumentCaptor.forClass(byte[].class);
        verify(client).put(eq(cursorKey), serialized.capture(), eq(Set.of(
                PutOption.PartitionKey(TOPIC), PutOption.IfVersionIdEquals(11L))));
        assertEquals(claim.offset(), CompactOffsetSerde.INSTANCE.deserialize(serialized.getValue()));
    }

    @Test
    void rootJsonNullPreparedClaimAndRepairAreRecoveryFailures() throws Exception {
        CompactedOffset legacy = new CompactedOffset(STREAM_ID, 9L, 0L);
        byte[] serializedLegacy = CompactOffsetSerde.INSTANCE.serialize(legacy);
        byte[] rootJsonNull = "null".getBytes(StandardCharsets.UTF_8);
        AsyncOxiaClient client = publicationMetadataClient(serializedLegacy, rootJsonNull);
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);

        assertThrows(PublicationRecoveryException.class,
                () -> manager.getPreparedTaskClaim(TOPIC));
        assertThrows(PublicationRecoveryException.class,
                () -> manager.repairLegacyPublishedOffset(lease()));

        verify(client, never()).put(eq("compact-offset-" + TOPIC), any(), anySet());
    }

    @Test
    void runtimePreparedDecodeFailureIsRecoveryFailure() throws Exception {
        CompactedOffset legacy = new CompactedOffset(STREAM_ID, 9L, 0L);
        AsyncOxiaClient client = publicationMetadataClient(
                CompactOffsetSerde.INSTANCE.serialize(legacy), null);
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);

        PublicationRecoveryException claimError = assertThrows(
                PublicationRecoveryException.class,
                () -> manager.getPreparedTaskClaim(TOPIC));
        PublicationRecoveryException repairError = assertThrows(
                PublicationRecoveryException.class,
                () -> manager.repairLegacyPublishedOffset(lease()));

        assertTrue(claimError.getCause() instanceof NullPointerException);
        assertTrue(repairError.getCause() instanceof NullPointerException);
        verify(client, never()).put(eq("compact-offset-" + TOPIC), any(), anySet());
    }

    @Test
    void normalOffsetUpdatesRejectPublishedCursorWithoutCumulativeSize() {
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);

        assertThrows(IllegalArgumentException.class,
                () -> manager.updatePublishedOffset(STREAM_ID, 9L, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> manager.updatePublishedOffset(TOPIC, STREAM_ID, 9L, 0L));

        verify(client, never()).put(anyString(), any(), anySet());
    }

    @Test
    void cursorCasRejectsPublishedCursorWithoutCumulativeSize() {
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);
        CompactTaskManager.PublishedOffsetClaim current =
                new CompactTaskManager.PublishedOffsetClaim(
                        new CompactedOffset(STREAM_ID, 9L, 100L), 11L);

        assertThrows(IllegalArgumentException.class,
                () -> manager.compareAndSetPublishedOffset(
                        lease(), current, new CompactedOffset(STREAM_ID, 19L, 0L)));

        verify(client, never()).put(anyString(), any(), anySet());
    }

    @Test
    void cursorCasRejectsLegacyExpectedCursor() {
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);
        CompactTaskManager.PublishedOffsetClaim legacy =
                new CompactTaskManager.PublishedOffsetClaim(
                        new CompactedOffset(STREAM_ID, 9L, 0L), 11L);

        assertThrows(LegacyPublishedOffsetException.class,
                () -> manager.compareAndSetPublishedOffset(
                        lease(), legacy, new CompactedOffset(STREAM_ID, 19L, 250L)));

        verify(client, never()).put(anyString(), any(), anySet());
    }

    @Test
    void preparedClaimDeleteUsesCreatedRevision() throws Exception {
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        when(client.put(anyString(), any(), anySet())).thenAnswer(invocation ->
                CompletableFuture.completedFuture(new PutResult(invocation.getArgument(0), version(21L))));
        when(client.delete(anyString(), anySet())).thenReturn(CompletableFuture.completedFuture(true));
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);
        PreparedCompactStreamTask task = PreparedCompactStreamTask.builder()
                .streamId(STREAM_ID)
                .startOffset(0L)
                .endOffset(10L)
                .taskName("task")
                .topic(TOPIC)
                .properties(Map.of())
                .build();

        CompactTaskManager.PreparedTaskClaim claim =
                manager.tryCreatePreparedTaskClaim(task, TOPIC).orElseThrow();
        assertTrue(manager.deletePreparedTaskClaim(TOPIC, claim));

        verify(client).delete(eq("prepared-task-" + TOPIC), eq(Set.of(
                DeleteOption.PartitionKey(TOPIC), DeleteOption.IfVersionIdEquals(21L))));
    }

    @Test
    void stalePreparedDeleteCannotRemoveSuccessorRevision() throws Exception {
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        when(client.delete(anyString(), anySet())).thenReturn(CompletableFuture.failedFuture(
                new UnexpectedVersionIdException("prepared-task-" + TOPIC, 22L)));
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);
        PreparedCompactStreamTask task = PreparedCompactStreamTask.builder()
                .streamId(STREAM_ID)
                .startOffset(0L)
                .endOffset(10L)
                .taskName("old-task")
                .topic(TOPIC)
                .properties(Map.of())
                .build();
        CompactTaskManager.PreparedTaskClaim stale =
                new CompactTaskManager.PreparedTaskClaim(task, 21L);

        assertFalse(manager.deletePreparedTaskClaim(TOPIC, stale));

        verify(client).delete(eq("prepared-task-" + TOPIC), eq(Set.of(
                DeleteOption.PartitionKey(TOPIC), DeleteOption.IfVersionIdEquals(21L))));
    }

    @Test
    void emptyPackageDeletionRechecksSubtasksAndUsesMarkerRevision() throws Exception {
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        String taskName = "orphan";
        String markerKey = "/compact-stream-tasks/" + taskName;
        when(client.get(markerKey)).thenReturn(CompletableFuture.completedFuture(
                new GetResult(markerKey, new byte[0], version(31L))));
        when(client.list(markerKey + "/", markerKey + "/\uffff"))
                .thenReturn(CompletableFuture.completedFuture(List.of()));
        when(client.delete(markerKey, Set.of(DeleteOption.IfVersionIdEquals(31L))))
                .thenReturn(CompletableFuture.completedFuture(true));
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);

        assertTrue(manager.deletePackagedTaskNameIfEmpty(taskName).get());

        verify(client).delete(markerKey, Set.of(DeleteOption.IfVersionIdEquals(31L)));
    }

    @Test
    void emptyPackageDeletionRetainsRewrittenMarker() throws Exception {
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        String taskName = "rewritten";
        String markerKey = "/compact-stream-tasks/" + taskName;
        when(client.get(markerKey)).thenReturn(CompletableFuture.completedFuture(
                new GetResult(markerKey, new byte[0], version(31L))));
        when(client.list(markerKey + "/", markerKey + "/\uffff"))
                .thenReturn(CompletableFuture.completedFuture(List.of()));
        when(client.delete(markerKey, Set.of(DeleteOption.IfVersionIdEquals(31L))))
                .thenReturn(CompletableFuture.failedFuture(
                        new UnexpectedVersionIdException(markerKey, 32L)));
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);

        assertFalse(manager.deletePackagedTaskNameIfEmpty(taskName).get());

        verify(client).delete(markerKey, Set.of(DeleteOption.IfVersionIdEquals(31L)));
    }

    @Test
    void emptyPackageDeletionRetainsMarkerWhenSubtaskAppears() throws Exception {
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        String taskName = "still-visible";
        String markerKey = "/compact-stream-tasks/" + taskName;
        when(client.get(markerKey)).thenReturn(CompletableFuture.completedFuture(
                new GetResult(markerKey, new byte[0], version(32L))));
        when(client.list(markerKey + "/", markerKey + "/\uffff"))
                .thenReturn(CompletableFuture.completedFuture(List.of(markerKey + "/42-0-10")));
        OxiaCompactTaskManager manager = new OxiaCompactTaskManager(client);

        assertFalse(manager.deletePackagedTaskNameIfEmpty(taskName).get());

        verify(client, never()).delete(eq(markerKey), anySet());
    }

    private static Version version(long versionId) {
        return new Version(versionId, 0L, 0L, 0L, Optional.empty(), Optional.empty());
    }

    private static CompactTaskManager.PublicationLease lease() {
        return new CompactTaskManager.PublicationLease(TOPIC, STREAM_ID, "owner", 7L);
    }

    private static PreparedCompactStreamTask preparedTask(
            long startOffset, long endOffset, long totalSize, long cumulativeSize) {
        return PreparedCompactStreamTask.builder()
                .streamId(STREAM_ID)
                .startOffset(startOffset)
                .endOffset(endOffset)
                .totalSize(totalSize)
                .cumulativeSize(cumulativeSize)
                .status(PreparedCompactStreamTask.INIT)
                .taskName("prepared-task")
                .topic(TOPIC)
                .properties(Map.of())
                .build();
    }

    private static AsyncOxiaClient repairClient(
            CompactedOffset cursor, PreparedCompactStreamTask preparedTask) throws Exception {
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        CompactTaskManager.PublicationLease lease = lease();
        byte[] leaseValue = (STREAM_ID + "\n" + lease.ownerId()).getBytes(StandardCharsets.UTF_8);
        GetResult leaseResult = new GetResult(
                "publication-lease", leaseValue, version(lease.revision()));
        GetResult cursorResult = new GetResult(
                "compact-offset-" + TOPIC,
                CompactOffsetSerde.INSTANCE.serialize(cursor),
                version(11L));
        GetResult preparedResult = preparedTask == null
                ? null
                : new GetResult(
                        "prepared-task-" + TOPIC,
                        PreparedCompactStreamTaskSerde.INSTANCE.serialize(preparedTask),
                        version(21L));
        when(client.get(anyString(), anySet())).thenAnswer(invocation -> {
            String key = invocation.getArgument(0);
            if (key.startsWith("publication-lease-")) {
                return CompletableFuture.completedFuture(leaseResult);
            }
            if (key.startsWith("compact-offset-")) {
                return CompletableFuture.completedFuture(cursorResult);
            }
            if (key.startsWith("prepared-task-")) {
                return CompletableFuture.completedFuture(preparedResult);
            }
            throw new AssertionError("Unexpected key: " + key);
        });
        when(client.put(eq("compact-offset-" + TOPIC), any(), anySet()))
                .thenReturn(CompletableFuture.completedFuture(
                        new PutResult("compact-offset-" + TOPIC, version(12L))));
        return client;
    }

    private static AsyncOxiaClient publicationMetadataClient(
            byte[] cursorContent, byte[] preparedContent) {
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        CompactTaskManager.PublicationLease lease = lease();
        byte[] leaseValue = (STREAM_ID + "\n" + lease.ownerId()).getBytes(StandardCharsets.UTF_8);
        GetResult leaseResult = new GetResult(
                "publication-lease", leaseValue, version(lease.revision()));
        GetResult cursorResult = metadataResult(
                "compact-offset-" + TOPIC, cursorContent, 11L);
        GetResult preparedResult = metadataResult(
                "prepared-task-" + TOPIC, preparedContent, 21L);
        when(client.get(anyString(), anySet())).thenAnswer(invocation -> {
            String key = invocation.getArgument(0);
            if (key.startsWith("publication-lease-")) {
                return CompletableFuture.completedFuture(leaseResult);
            }
            if (key.startsWith("compact-offset-")) {
                return CompletableFuture.completedFuture(cursorResult);
            }
            if (key.startsWith("prepared-task-")) {
                return CompletableFuture.completedFuture(preparedResult);
            }
            throw new AssertionError("Unexpected key: " + key);
        });
        return client;
    }

    private static GetResult metadataResult(String key, byte[] content, long versionId) {
        if (content != null) {
            return new GetResult(key, content, version(versionId));
        }
        GetResult result = mock(GetResult.class);
        when(result.key()).thenReturn(key);
        when(result.value()).thenReturn(null);
        when(result.version()).thenReturn(version(versionId));
        return result;
    }
}
