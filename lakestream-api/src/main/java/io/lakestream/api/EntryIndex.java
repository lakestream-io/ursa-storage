/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Represents the Entry Index information.
 */
@Data
@AllArgsConstructor
@Accessors(fluent = true)
public class EntryIndex implements LogEntryIndex {

    /**
     * A header together with its position index within the entry offsets array.
     */
    public record HeaderWithIndex(EntryHeader header, int index) {
    }

    public static final EntryIndex NOT_FOUND = new EntryIndex(EntryHeader.NOT_FOUND, Position.NOT_FOUND,
        -1, null, Optional.empty(), Optional.empty(), Optional.empty());

    public enum IndexType {
        NORMAL,
        COMPACT,
    }

    private final EntryHeader header;
    private final Position position;
    private final int entryCount;
    private final IndexType indexType;
    @Setter
    volatile Optional<int[]> entryOffsets; /// the number of entries
    @Setter
    volatile Optional<Map<Long, HeaderWithIndex>> entryHeaders; /// the number of entries
    private final Optional<Map<String, String>> extraData;

    public EntryIndex(EntryHeader header, Position position, int entryCount, IndexType indexType,
                      Optional<int[]> entryOffsets) {
        this(header, position, entryCount, indexType, entryOffsets,
            buildEntryHeaders(entryOffsets, header, entryCount), Optional.empty());
    }

    public EntryIndex(EntryHeader header, Position position, int entryCount, IndexType indexType,
                      Optional<int[]> entryOffsets, Optional<Map<String, String>> extraData) {
        this(header, position, entryCount, indexType, entryOffsets,
            buildEntryHeaders(entryOffsets, header, entryCount), extraData);
    }

    public static EntryIndex of(EntryHeader header,
                                Position position,
                                int entryCount,
                                int storageObjectCount) {
        return new EntryIndex(header, position, entryCount,
            storageObjectCount == 1 ? IndexType.COMPACT : IndexType.NORMAL, Optional.empty(), Optional.empty(),
            Optional.empty());
    }

    private static Optional<Map<Long, HeaderWithIndex>> buildEntryHeaders(Optional<int[]> offsets,
                                                                      EntryHeader header,
                                                                      int entryCount) {
        if (offsets.isEmpty()) {
            return Optional.empty();
        }
        long startOffset = header.offset();
        var arr = offsets.get();

        // we estimate byte size as header.size should be used for stat only.
        // If accurate byte size is required, we need to additionally store size info in the oxia entry index
        int estimatedSizeInBytes = header.entrySize() / entryCount;
        long estimatedCumulativeSizeInBytes = header.cumulativeSize() - header.entrySize();
        int offset = 0;
        Map<Long, HeaderWithIndex> map = new HashMap<>();
        for (int i = 0; i < arr.length; i++) {
            int endOffset = arr[i];
            int count = endOffset - offset;
            var key = offset + startOffset;
            estimatedCumulativeSizeInBytes += estimatedSizeInBytes;
            var h = new EntryHeader(offset + startOffset, count,
                    header.writtenTimestamp(),
                    estimatedSizeInBytes,
                    estimatedCumulativeSizeInBytes);
            map.put(key, new HeaderWithIndex(h, i));
            offset = endOffset;
        }
        return Optional.of(map);
    }

    private static void validateSearchedOffset(long targetOffset, EntryHeader header) {
        if (header == EntryHeader.NOT_FOUND) {
            return;
        }
        if (header.offset() <= targetOffset
                && targetOffset < header.offset() + header.numberOfRecords()) {
            return;
        }
        throw new IllegalStateException(
                "target offset:" + targetOffset + " is not in the header offset range:" + header);
    }

    private static void validateOffset(long targetOffset, EntryHeader header) {
        if (header == EntryHeader.NOT_FOUND) {
            return;
        }
        if (header.offset() == targetOffset) {
            return;
        }
        throw new IllegalStateException(
                "target offset:" + targetOffset + " is not the header offset:" + header);
    }

    public EntryHeader searchEntryHeader(long targetOffset) {
        var header = tryGetCommonHeader();
        if (header != null) {
            validateSearchedOffset(targetOffset, header);
            return header;
        }
        var offset = searchEntryOffset(targetOffset);
        header = doGetEntryHeader(offset);
        validateSearchedOffset(targetOffset, header);
        return header;
    }

    public EntryHeader getFirstEntryHeader() {
        var header = tryGetCommonHeader();
        if (header != null) {
            return header;
        }
        var offset = getFirstEntryOffset();
        header = doGetEntryHeader(offset);
        return header;
    }

    public EntryHeader getLastEntryHeader() {
        var header = tryGetCommonHeader();
        if (header != null) {
            return header;
        }
        var offset = getLastEntryOffset();
        header = doGetEntryHeader(offset);
        return header;
    }

    public EntryHeader getEntryHeader(long offset) {
        var header = tryGetCommonHeader();
        if (header != null) {
            validateOffset(offset, header);
            return header;
        }
        header = doGetEntryHeader(offset);
        validateOffset(offset, header);
        return header;
    }

    private EntryHeader doGetEntryHeader(long offset) {
        if (entryHeaders.isEmpty()) {
            throw new IllegalStateException("entryHeaders not found at" + offset);
        }
        var header = entryHeaders.get().get(offset);

        if (header == null) {
            throw new IllegalStateException("header not found at:" + offset);
        }

        return header.header();
    }

    private long getFirstEntryOffset() {
        return header.offset();
    }

    private long getLastEntryOffset() {
        var arr = entryOffsets.get();
        return arr[arr.length - 2] + header.offset(); // at least two entries
    }


    private EntryHeader tryGetCommonHeader() {
        if (this == EntryIndex.NOT_FOUND) {
            return header;
        }
        if (entryCount <= 1) {
            return header;
        }
        return null;
    }

    private long searchEntryOffset(long offset) {
        int target = (int) (offset - header.offset());
        if (target < 0) {
            throw new IllegalStateException("offset not found at" + offset + " target " + target);
        }
        if (entryOffsets.isEmpty()) {
            throw new IllegalStateException("entryOffsets not found at" + offset + " target " + target);
        }
        // [10,20]
        var arr = entryOffsets.get();
        int index = Arrays.binarySearch(arr, target);

        if (index >= 0) {
            if (index >= arr.length - 1) {
                throw new IllegalStateException(
                        "offset found at the end of entryOffsets" + offset + " from " + index);
            }
            return arr[index] + header.offset();
        }

        index = -(index + 1);

        if (index == 0) {
            return header.offset();
        }

        if (index >= arr.length) {
            throw new IllegalStateException("offset not found at" + offset + " from " + index);
        }

        return arr[index - 1] + header.offset();
    }

}
