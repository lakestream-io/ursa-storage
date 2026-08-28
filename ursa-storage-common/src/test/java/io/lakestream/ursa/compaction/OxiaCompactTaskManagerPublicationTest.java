/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.ursa.compaction.task.CompactOffsetSerde;
import io.lakestream.ursa.compaction.task.CompactedOffset;
import io.lakestream.ursa.compaction.task.PreparedCompactStreamTask;
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
        CompactedOffset cursor = new CompactedOffset(STREAM_ID, 99L, 0L);
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

    private static Version version(long versionId) {
        return new Version(versionId, 0L, 0L, 0L, Optional.empty(), Optional.empty());
    }
}
