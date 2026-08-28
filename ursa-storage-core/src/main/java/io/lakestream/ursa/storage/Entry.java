/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.LogEntry;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Represents an entry in the Ursa storage system.
 * This class is a record, providing a concise way to create an immutable data transfer object.
 *
 * An Entry consists of two parts:
 * 1. A header containing metadata about the entry.
 * 2. A payload containing the actual data.
 *
 * This class is used throughout the storage system, particularly in:
 * - WalStorage implementation for writing and reading entries.
 * - StorageApi implementation for operations like appending and reading entries.
 * - CompactionService for processing and compacting entries.
 *
 * Usage considerations:
 * - Ensure proper management of the ByteBuf payload, as it may need to be released when no longer needed.
 * - When creating or manipulating Entry objects, be aware of the underlying ByteBuf's reference counting.
 * - This class is designed for efficient data transfer and should not be modified after creation.
 * - When working with Entry objects, especially when storing or caching them, be mindful of the ByteBuf
 *   reference counting to prevent memory leaks or premature release of the buffer.
 */
public record Entry(
        EntryHeader header,
        ByteBuf payload
) {
    public static Entry of(EntryHeader header, ByteBuf payload) {
        return new Entry(header, payload);
    }

    public Entry retainedDuplicate() {
        return new Entry(header, payload.retainedDuplicate());
    }

    /**
     * Transfers the payload references owned by the supplied entries to closeable {@link LogEntry} views.
     * If conversion fails, all transferred and untransferred payload references are released.
     */
    public static List<LogEntry> toLogEntries(List<Entry> entries) {
        List<LogEntry> result = new ArrayList<>(entries.size());
        int nextUntransferredEntry = 0;
        try {
            while (nextUntransferredEntry < entries.size()) {
                Entry entry = entries.get(nextUntransferredEntry);
                if (entry == null) {
                    throw new IllegalArgumentException("Entry list must not contain null entries");
                }
                LogEntry converted = entry.toLogEntry();
                nextUntransferredEntry++;
                try {
                    result.add(converted);
                } catch (RuntimeException | Error addFailure) {
                    try {
                        converted.close();
                    } catch (RuntimeException | Error cleanupFailure) {
                        addFailure.addSuppressed(cleanupFailure);
                    }
                    throw addFailure;
                }
            }
            return result;
        } catch (RuntimeException | Error conversionError) {
            for (LogEntry entry : result) {
                try {
                    entry.close();
                } catch (RuntimeException | Error cleanupError) {
                    conversionError.addSuppressed(cleanupError);
                }
            }
            for (int i = nextUntransferredEntry; i < entries.size(); i++) {
                Entry entry = entries.get(i);
                if (entry == null || entry.payload() == null) {
                    continue;
                }
                try {
                    entry.payload().release();
                } catch (RuntimeException | Error cleanupError) {
                    conversionError.addSuppressed(cleanupError);
                }
            }
            throw conversionError;
        }
    }

    /**
     * Returns a {@link LogEntry} view of this entry.
     *
     * <p>Ownership of this entry's existing payload reference is transferred to the returned
     * {@code LogEntry}. This method does not retain or copy the payload. The caller must not use
     * or release this {@code Entry} after conversion and must close the returned entry.
     *
     * @return a LogEntry view of this entry
     */
    public LogEntry toLogEntry() {
        return new LogEntry() {
            private final AtomicBoolean closed = new AtomicBoolean(false);

            @Override
            public long offset() {
                return header.offset();
            }

            @Override
            public int numberOfRecords() {
                return header.numberOfMessages();
            }

            @Override
            public long timestamp() {
                return header.writtenTimestamp();
            }

            @Override
            public int size() {
                return header.entrySize();
            }

            @Override
            public ByteBuf payload() {
                return Entry.this.payload.asReadOnly().duplicate();
            }

            @Override
            public void close() {
                if (closed.compareAndSet(false, true)) {
                    Entry.this.payload.release();
                }
            }
        };
    }
}
