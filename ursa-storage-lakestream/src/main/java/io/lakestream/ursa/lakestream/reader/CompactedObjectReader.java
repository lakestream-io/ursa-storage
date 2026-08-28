/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.reader;

import io.lakestream.api.EntryIndex;
import io.lakestream.api.LogEntry;
import io.lakestream.ursa.compaction.common.ManagedTableFileIndex;
import io.lakestream.ursa.storage.Entry;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface CompactedObjectReader {

    /**
     * Result of a read operation from a {@link CompactedObjectReader}.
     *
     * <p>Semantics:
     * <ul>
     *   <li>If {@code isV2Result} is {@code false}, this represents a v1 result and
     *   {@code entries} will contain exactly one {@link LogEntry}.</li>
     *   <li>If {@code isV2Result} is {@code true}, this represents a v2 result and
     *   {@code entries} may contain zero or more {@link LogEntry} instances.</li>
     * </ul>
     *
     * @param isV2Result whether the result follows the v2 format
     * @param entries the entries read according to the semantics described above
     */
    record ReadResult(boolean isV2Result, List<LogEntry> entries) { }

    default Optional<ManagedTableFileIndex> getManagedTableFileIndex(EntryIndex entryIndex) {
        return Optional.empty();
    }

    default CompletableFuture<ReadResult> readMessagesWithEntryIndexAsync(EntryIndex entryIndex, long startOffset,
                                                                     long baseOffset, long maxNumOfMessages,
                                                                     long maxSize) {
        var location = entryIndex.position().location();
        return readMessagesAsync(location, startOffset, baseOffset, maxNumOfMessages, maxSize);
    }

    CompletableFuture<ReadResult> readMessagesAsync(String path, long startOffset, long baseOffset,
                                                    long maxNumOfMessages, long maxSize);

    boolean hasSpaceInCache();

    CompletableFuture<Entry> preFetchMessagesAsync(String path, long startOffset, long baseOffset,
                                                   long maxNumOfMessages, long maxSize, long estimatedSize);

    void close();
}
