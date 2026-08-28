/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction.common;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import java.util.TreeMap;
import lombok.extern.slf4j.Slf4j;

/**
 * ManagedTableFileIndex is used to manage the multiple file index in one oxia index. The index must append ordered
 * with the offsets.
 *
 * When topic schema changed, the data in one task will write into the different files. We need an index to check
 * where is the data located.
 * The index is a mapping of offset and file path. For example,
 *      offset : 10, file path: data1.parquet
 *      offset : 20, file path: data2.parquet
 * It means the first 10 messages in the data1.parquet, and the 10 to 20 in the data2.parquet.
 *
 * The schema evolution is not allowed happened to frequently because the data will separate into multiple files which
 * will impact the read performance. We can leverage the variant type in the future to improve that. So the index should
 * not too many.
 */
@Slf4j
public class ManagedTableFileIndex {

    public static final String NAME = "ManagedTableFileIndex";
    private ByteBuffer buffer;
    private boolean sealed = false;
    private final TreeMap<Long, String> resultCache = new TreeMap<>();

    public ManagedTableFileIndex(int initialCapacityBytes) {
        this.buffer = ByteBuffer.allocate(initialCapacityBytes);
    }

    public ManagedTableFileIndex() {
        this(4 * 1024);
    }

    public void append(long offset, String filePath) {
        log.info("Append index with offset {}, file {}", offset, filePath);
        byte[] data = filePath.getBytes(StandardCharsets.UTF_8);
        int needed = Long.BYTES  /* offset */
                     + Integer.BYTES /* length */
                     + data.length;  /* data */

        ensureCapacity(buffer.position() + needed);
        buffer.putLong(offset);
        buffer.putInt(data.length);
        buffer.put(data);
    }

    public String get(long offsetToFind) throws IllegalArgumentException {
        load();
        checkBounds(offsetToFind);
        return resultCache.ceilingEntry(offsetToFind).getValue();
    }

    public Optional<Long> getFileBaseOffset(long offsetToFind) throws IllegalArgumentException {
        load();
        checkBounds(offsetToFind);
        return Optional.ofNullable(resultCache.lowerKey(offsetToFind)).map(l -> l + 1);
    }

    private void checkBounds(long offsetToFind) {
        if (resultCache.isEmpty()) {
            throw new IllegalArgumentException("Index is empty");
        }
        if (offsetToFind < 0 || offsetToFind > resultCache.lastKey()) {
            throw new IllegalArgumentException("Offset out of bounds: " + offsetToFind);
        }
    }

    void load() {
        if (sealed && !resultCache.isEmpty()) {
            return;
        }
        ByteBuffer dup = buffer.asReadOnlyBuffer();
        dup.flip();
        while (dup.remaining() >= Long.BYTES + Integer.BYTES) {
            long off = dup.getLong();
            int len = dup.getInt();
            if (len < 0 || len > dup.remaining()) {
                break;
            }
            byte[] data = new byte[len];
            dup.get(data);
            var filePath = new String(data, StandardCharsets.UTF_8);
            resultCache.put(off, filePath);
        }
    }

    public String serializeToString() {
        ByteBuffer dup = buffer.asReadOnlyBuffer();
        dup.flip();
        byte[] bytes = new byte[dup.limit()];
        dup.get(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }

    public static ManagedTableFileIndex deserializeFromString(String base64, int initialCapacityBytes) {
        byte[] bytes = Base64.getDecoder().decode(base64);
        ManagedTableFileIndex idx = new ManagedTableFileIndex(bytes.length);
        idx.sealed = true;
        idx.buffer.put(bytes);
        return idx;
    }

    public static ManagedTableFileIndex deserializeFromString(String base64) {
        return deserializeFromString(base64, 4 * 1024);
    }

    private void ensureCapacity(int minCapacity) {
        if (minCapacity <= buffer.capacity()) {
            return;
        }
        int newCap = Math.max(buffer.capacity() * 2, minCapacity);
        ByteBuffer newBuf = ByteBuffer.allocate(newCap);
        buffer.flip();
        newBuf.put(buffer);
        buffer = newBuf;
    }

}
