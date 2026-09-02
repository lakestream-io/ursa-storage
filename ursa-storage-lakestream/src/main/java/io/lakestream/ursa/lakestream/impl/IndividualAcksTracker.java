/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import com.google.common.annotations.VisibleForTesting;
import io.lakestream.ursa.utils.FutureUtils;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.RangeScanConsumer;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.RangeScanOption;
import io.oxia.client.api.options.defs.OptionPartitionKey;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class IndividualAcksTracker implements Closeable {

    @Nullable private final AsyncOxiaClient oxia;
    private final long cursorId;
    private final String basePath;

    private final TreeMap<Long, IndividualAcksTrackerSegment> segments = new TreeMap<>();
    private final List<IndividualAcksTrackerSegment> segmentsToDelete = new LinkedList<>();

    final Set<PutOption> putOptions;
    final Set<GetOption> getOptions;
    final Set<DeleteOption> deleteOptions;
    final Set<RangeScanOption> rangeScanOptionsOptions;

    private static final long DEFAULT_SEGMENT_BOUNDARY = 100_000_000;

    // The bitmap works on ints over a base offset, so we cannot stretch it more than 2B offsets
    private static final int DEFAULT_MAX_SEGMENT_SPAN = (int) 1e9;
    private static final long DEFAULT_MAX_SEGMENT_SIZE = 1024 * 1024;

    private final long maxSegmentSpan;
    private final long maxSegmentSize;

    public IndividualAcksTracker(AsyncOxiaClient oxia, long cursorId, boolean durable) {
        this(oxia, cursorId, DEFAULT_MAX_SEGMENT_SPAN, DEFAULT_MAX_SEGMENT_SIZE, durable);
    }

    @VisibleForTesting
    public IndividualAcksTracker(AsyncOxiaClient oxia, long cursorId, long maxSegmentSpan, long maxSegmentSize,
                                 boolean durable) {
        this.oxia = durable ? oxia : null;
        this.cursorId = cursorId;
        this.basePath = String.format("individual-acks-%020d", cursorId);
        this.maxSegmentSpan = maxSegmentSpan;
        this.maxSegmentSize = maxSegmentSize;

        // All keys are going to be partitioned based on same cursor id
        var partitionKey = new OptionPartitionKey(String.format("%d", cursorId));
        this.putOptions = Collections.singleton(partitionKey);
        this.getOptions = Collections.singleton(partitionKey);
        this.deleteOptions = Collections.singleton(partitionKey);
        this.rangeScanOptionsOptions = Collections.singleton(partitionKey);
    }

    public CompletableFuture<Void> initialize() {
        if (oxia == null) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        // Load all the segments
        oxia.rangeScan(basePath, basePath + "-9", new RangeScanConsumer() {
            @Override
            public boolean onNext(GetResult result) {
                var segment =
                        IndividualAcksTrackerSegment.parseFromValue(IndividualAcksTracker.this, result.value());
                segments.put(segment.getBaseOffset(), segment);
                return true;
            }

            @Override
            public void onError(Throwable throwable) {
                future.completeExceptionally(throwable);
            }

            @Override
            public void onCompleted() {
                future.complete(null);
            }
        });
        return future;
    }

    public long count() {
        if (segments.isEmpty()) {
            return 0;
        }

        return segments.values().stream()
                .mapToLong(IndividualAcksTrackerSegment::count)
                .sum();
    }

    public long countFromRange(long from, long to) {
        if (segments.isEmpty()) {
            return 0;
        }

        return segments.values().stream()
                .mapToLong(s -> s.countFromRange(from, to))
                .sum();
    }

    public CompletableFuture<Void> remove() {
        return FutureUtils.waitForAll(
                segments.values().stream()
                        .map(IndividualAcksTrackerSegment::remove)
                        .toList()
        );
    }

    public void deleteOffset(long offset) {
        deleteOffset(offset, null);
    }

    public void deleteOffset(long offset, long[] ackSet) {
        if (segments.isEmpty()) {
            long baseOffset = offset / DEFAULT_SEGMENT_BOUNDARY;
            segments.put(baseOffset, new IndividualAcksTrackerSegment(this, baseOffset));
        }

        IndividualAcksTrackerSegment segment = getOrFirstSegment(offset);
        if (ackSet != null) {
            segment.addFromAckSet(offset, ackSet);
        } else {
            segment.addOffset(offset);
        }
    }

    public long firstNonDeletedOffset(long markDeleteOffset) {
        if (segments.isEmpty()) {
            return markDeleteOffset + 1;
        } else if (segments.size() == 1) {
            return segments.firstEntry().getValue().firstNonDeletedOffset(markDeleteOffset);
        }

        long firstNonDeletedOffset = markDeleteOffset;
        for (var segment : segments.values()) {
            // There might be more chances to advance in next segment
            firstNonDeletedOffset = segment.firstNonDeletedOffset(firstNonDeletedOffset);
            long lastOffset = segment.lastOffset();
            if (firstNonDeletedOffset - 1 < lastOffset) {
                break;
            }

        }
        return firstNonDeletedOffset;
    }

    public void trimToOffset(long offset) {
        if (segments.isEmpty()) {
            return;
        }

        if (segments.size() == 1) {
            var segment = segments.firstEntry().getValue();
            segment.trimToOffset(offset);
            if (segment.isEmpty()) {
                segments.pollFirstEntry();
                segmentsToDelete.add(segment);
            }
        } else {
            for (var it = segments.entrySet().iterator(); it.hasNext(); ) {
                var segment = it.next().getValue();

                if ((segment.isEmpty() || offset >= segment.lastOffset()) && it.hasNext()) {
                    // We have moved the baseline after a segment which is not the last
                    // we can thus get rid of this segment
                    it.remove();
                    segmentsToDelete.add(segment);
                    continue;
                }

                segment.trimToOffset(offset);
                if (segment.isEmpty() && it.hasNext()) {
                    it.remove();
                    segmentsToDelete.add(segment);
                }
            }
        }
    }

    public record OffsetRange(long start, long end) {
    }

    public OffsetRange lastRange() {
        if (segments.isEmpty()) {
            return null;
        }
        IndividualAcksTrackerSegment segment = segments.size() == 1
                ? segments.firstEntry().getValue()
                : segments.lastEntry().getValue();
        int end = segment.getBitmap().last();
        long start = segment.getBitmap().previousAbsentValue(end);
        return new OffsetRange(segment.getBaseOffset() + start, segment.getBaseOffset() + end);
    }

    private IndividualAcksTrackerSegment getSegment(long offset) {
        var entry = segments.floorEntry(offset);
        if (entry == null) {
            return null;
        }

        return entry.getValue();
    }

    private IndividualAcksTrackerSegment getOrFirstSegment(long offset) {
        if (segments.size() == 1) {
            return segments.firstEntry().getValue();
        }
        var entry = segments.floorEntry(offset);
        return entry != null ? entry.getValue() : segments.firstEntry().getValue();
    }

    public CompletableFuture<Void> flush() {
        var lastEntry = segments.lastEntry();
        if (lastEntry != null) {
            var segment = lastEntry.getValue();
            var span = segment.span();
            if (segment.getSegmentSize() > maxSegmentSize
                    || span > maxSegmentSpan) {
                // Create a new segment
                var newBaseOffset = segment.getBaseOffset() + span + 1;
                segments.put(newBaseOffset, new IndividualAcksTrackerSegment(this, newBaseOffset));
            }
        }

        return FutureUtils.waitForAll(
                segments.values().stream()
                        .map(IndividualAcksTrackerSegment::flush)
                        .toList()
        ).thenCompose(v -> FutureUtils.waitForAll(
                segmentsToDelete.stream()
                        .map(IndividualAcksTrackerSegment::remove)
                        .toList()
        ));
    }

    @Override
    public void close() throws IOException {
        flush().join();
    }

    public boolean isEmpty() {
        return segments.isEmpty();
    }

    public boolean contains(long offset) {
        if (segments.isEmpty()) {
            return false;
        } else {
            var segment = getSegment(offset);
            return segment != null && segment.contains(offset);
        }
    }

    public void clearAfterOffset(long offset) {
        if (segments.isEmpty()) {
            return;
        }
        NavigableMap<Long, IndividualAcksTrackerSegment> segments =
                this.segments.tailMap(offset, true);
        segments.forEach((key, value) -> {
            value.clearAfterOffset(offset);
        });
    }

}
