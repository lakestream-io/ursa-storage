/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.compaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.CompactionManager;
import io.lakestream.ursa.compaction.PublicationFencedException;
import io.lakestream.ursa.compaction.task.CompactedOffset;
import io.lakestream.ursa.compaction.task.PreparedCompactStreamTask;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class MemoryCompactTaskManagerPublicationTest {

    private static final String TOPIC = "org/analytics/orders";

    @Test
    void publicationLeaseIsExclusiveAndReleaseIsConditional() throws Exception {
        MemoryCompactTaskManager manager = new MemoryCompactTaskManager();
        CompactTaskManager.PublicationLease first =
                manager.tryAcquirePublicationLease(TOPIC, 1L).orElseThrow();

        assertTrue(manager.tryAcquirePublicationLease(TOPIC, 1L).isEmpty());
        CompactTaskManager.PublicationLease stale = new CompactTaskManager.PublicationLease(
                TOPIC, 1L, first.ownerId(), first.revision() + 1);
        assertFalse(manager.releasePublicationLease(stale));
        assertTrue(manager.validatePublicationLease(first));

        assertTrue(manager.releasePublicationLease(first));
        assertTrue(manager.tryAcquirePublicationLease(TOPIC, 2L).isPresent());
    }

    @Test
    void successorCursorClaimFencesOldSessionAndResetsIncarnationOnce() throws Exception {
        MemoryCompactTaskManager manager = new MemoryCompactTaskManager();
        CompactTaskManager.PublicationLease oldLease =
                manager.tryAcquirePublicationLease(TOPIC, 1L).orElseThrow();
        CompactTaskManager.PublishedOffsetClaim oldCursor = manager.claimPublishedOffset(oldLease);
        oldCursor = manager.compareAndSetPublishedOffset(
                oldLease, oldCursor, new CompactedOffset(1L, 99L, 0L));
        assertTrue(manager.releasePublicationLease(oldLease));

        CompactTaskManager.PublicationLease newLease =
                manager.tryAcquirePublicationLease(TOPIC, 2L).orElseThrow();
        CompactTaskManager.PublishedOffsetClaim newCursor = manager.claimPublishedOffset(newLease);

        assertEquals(new CompactedOffset(2L, -1L, 0L), newCursor.offset());
        CompactTaskManager.PublishedOffsetClaim staleCursor = oldCursor;
        assertThrows(PublicationFencedException.class,
                () -> manager.compareAndSetPublishedOffset(
                        oldLease, staleCursor, new CompactedOffset(1L, 199L, 0L)));
        assertEquals(new CompactedOffset(2L, -1L, 0L), manager.getPublishedOffset(TOPIC));
    }

    @Test
    void stalePreparedDeleteCannotRemoveSuccessorClaim() throws Exception {
        MemoryCompactTaskManager manager = new MemoryCompactTaskManager();
        PreparedCompactStreamTask firstTask = task(1L, 0L, 10L, "first");
        CompactTaskManager.PreparedTaskClaim first =
                manager.tryCreatePreparedTaskClaim(firstTask, TOPIC).orElseThrow();
        assertTrue(manager.deletePreparedTaskClaim(TOPIC, first));
        PreparedCompactStreamTask secondTask = task(2L, 0L, 10L, "second");
        CompactTaskManager.PreparedTaskClaim second =
                manager.tryCreatePreparedTaskClaim(secondTask, TOPIC).orElseThrow();

        assertFalse(manager.deletePreparedTaskClaim(TOPIC, first));
        assertEquals(second, manager.getPreparedTaskClaim(TOPIC).orElseThrow());
    }

    @Test
    void secondPublisherReadsCommittedCursorAndDoesNotDuplicateRange() throws Exception {
        MemoryCompactTaskManager taskManager = new MemoryCompactTaskManager();
        CompactionManager manager = new CompactionManager(taskManager);
        CompactionManager.PublicationSession first =
                manager.tryOpenPublicationSession(TOPIC, 1L).orElseThrow();

        assertEquals(CompactionManager.PublicationResult.PUBLISHED,
                first.publishNext(last -> Optional.of(task(1L, last + 1, 100L, "first"))));
        first.close();

        CompactionManager.PublicationSession second =
                manager.tryOpenPublicationSession(TOPIC, 1L).orElseThrow();
        assertEquals(CompactionManager.PublicationResult.NO_TASK,
                second.publishNext(last -> {
                    assertEquals(99L, last);
                    return Optional.empty();
                }));
        second.close();

        assertEquals(99L, taskManager.getPublishedOffset(TOPIC).getOffset());
        assertEquals(1, taskManager.getAllTasks().get().size());
    }

    private static PreparedCompactStreamTask task(long streamId, long startOffset, long endOffset, String taskName) {
        return PreparedCompactStreamTask.builder()
                .streamId(streamId)
                .startOffset(startOffset)
                .endOffset(endOffset)
                .taskName(taskName)
                .topic(TOPIC)
                .status(PreparedCompactStreamTask.INIT)
                .properties(Map.of())
                .build();
    }
}
