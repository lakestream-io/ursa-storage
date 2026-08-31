/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

public interface EntryProcessFactory extends AutoCloseable {

    default IEntryReader createEntryReader(String topic, long streamId, long startOffset, long endOffset,
                                           double avgEntrySize) {
        return createEntryReader(topic, streamId, startOffset, endOffset, avgEntrySize, EntryReaderOptions.DEFAULT);
    }

    default void cleanUp() {
    }

    IEntryReader createEntryReader(String topic, long streamId, long startOffset, long endOffset,
                                   double avgEntrySize, EntryReaderOptions options);

}
