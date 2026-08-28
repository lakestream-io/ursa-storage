/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

/**
 * Seam that supplies an {@link IEntryReader} for a stream's offset range. Lets
 * {@link LakehouseMaterializationService} read source entries through the configured source reader via the
 * production {@link EntryProcessFactory} while staying unit-testable with a stub reader.
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
