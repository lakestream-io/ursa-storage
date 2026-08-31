/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

/**
 * Test seam that supplies an {@link IEntryReader} for a stream's offset range. Production reads use
 * the StorageApi-backed {@link EntryProcessFactory}; tests may inject a stub reader.
 */
interface EntryReaderProvider {

    /**
     * Opens a reader over {@code [startOffset, endOffset)} for {@code topic}.
     *
     * @param topic       the source topic / stream full name
     * @param streamId    the stream id
     * @param startOffset inclusive start offset
     * @param endOffset   exclusive end offset
     * @return a reader whose {@link IEntryReader#read()} yields {@code GenericEntry}s until exhausted
     */
    IEntryReader create(String topic, long streamId, long startOffset, long endOffset) throws Exception;
}
