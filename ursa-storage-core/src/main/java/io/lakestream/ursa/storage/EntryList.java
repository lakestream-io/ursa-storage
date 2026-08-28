/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import io.lakestream.api.EntryHeader;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import lombok.Getter;
import lombok.Setter;

/**
 * Represents Entry List abstraction that contains read contexts(conditions) and output entries.
 */
public class EntryList {
    // input
    @Getter
    private final long streamId;
    @Getter
    private final long startOffset;
    private final long maxOffset;
    private final int maxMessageCount;
    private final long maxSizeInBytes;
    private final Predicate<Long> offsetDeletedCondition;
    private final Predicate<Long> skipCondition;

    // output
    @Getter
    private final List<Entry> entries = new ArrayList<>();
    @Getter
    private long sizeInBytes = 0;
    @Getter
    private long messageCount = 0;
    @Getter
    @Setter
    private int repeatEntryIndex = -1; // set if the copy was stopped in the middle due to size constraint


    public EntryList(long streamId) {
        this(streamId, 0, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE, null, null);
    }
    public EntryList(long streamId, long startReadOffset, int maxMessageCount, long maxSizeInBytes) {
        this(streamId, startReadOffset, Long.MAX_VALUE, maxMessageCount, maxSizeInBytes, null, null);
    }

    public EntryList(long streamId, long startReadOffset, long maxReadOffset, int maxMessageCount, long maxSizeInBytes,
                     Predicate<Long> offsetDeletedCondition, Predicate<Long> skipCondition) {
        this.streamId = streamId;
        this.startOffset = startReadOffset;
        this.maxOffset = maxReadOffset;
        this.maxMessageCount = maxMessageCount;
        this.maxSizeInBytes = maxSizeInBytes;
        this.offsetDeletedCondition = offsetDeletedCondition;
        this.skipCondition = skipCondition;
    }

    public boolean isNotFull(EntryHeader header) {
        if (entries.isEmpty()) {
            return true;
        }
        if (maxOffset >= 0 && header.offset() + header.numberOfMessages() > maxOffset) {
            return false;
        }
        if (maxMessageCount >= 0 && messageCount + header.numberOfMessages() > maxMessageCount) {
            return false;
        }

        if (maxSizeInBytes >= 0 && sizeInBytes + header.entrySize() > maxSizeInBytes) {
            return false;
        }
        return true;
    }


    public boolean shouldSkip(EntryHeader header) {
        var offset = header.offset();
        //[4, 5, 6), [6, 7, 8) => startOffset=6
        if (offset + header.numberOfMessages() <= startOffset) {
            return true;
        }

        if (offsetDeletedCondition != null && offsetDeletedCondition.test(offset)) {
            return true;
        }

        if (skipCondition != null && skipCondition.test(offset)) {
            return true;
        }
        return false;
    }

    public void add(Entry entry) {
        entries.add(entry);
        sizeInBytes += entry.header().entrySize();
        messageCount += entry.header().numberOfMessages();
    }

    public Entry get(int i) {
        return entries.get(i);
    }

    public void set(int i, Entry entry) {
        entries.set(i, entry);
    }

    public int size() {
        return entries.size();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public void clear() {
        for (var e : entries) {
            if (e != null && e.payload() != null) {
                e.payload().release();
            }
        }
        entries.clear();
        sizeInBytes = 0;
        messageCount = 0;
        repeatEntryIndex = -1;
    }
}
