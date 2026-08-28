/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import io.lakestream.ursa.exception.DataSourceException;
import java.util.Map;

public interface EntryProcessFactory extends AutoCloseable {

    default IEntryReader createEntryReader(String topic, long streamId, long startOffset, long endOffset,
                                           double avgEntrySize) {
        return createEntryReader(topic, streamId, startOffset, endOffset, avgEntrySize, EntryReaderOptions.DEFAULT);
    }

    default void cleanUp() {
    }

    IEntryReader createEntryReader(String topic, long streamId, long startOffset, long endOffset,
                                   double avgEntrySize, EntryReaderOptions options);

    /**
     * Creates a reader with the properties carried by the source task.
     *
     * <p>Source-specific factories can override this method when opening a reader requires task
     * identity metadata. The default keeps non-Kafka readers and callers source-compatible.
     */
    default IEntryReader createEntryReader(String topic, long streamId, long startOffset, long endOffset,
                                           double avgEntrySize, EntryReaderOptions options,
                                           Map<String, String> taskProperties) throws DataSourceException {
        return createEntryReader(topic, streamId, startOffset, endOffset, avgEntrySize, options);
    }
}
