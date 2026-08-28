/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.storage.StorageApi;

public class UrsaEntryProcessFactory implements EntryProcessFactory{

    private StorageApi storageApi;
    private CompactionMetrics metrics;

    UrsaEntryProcessFactory(StorageApi storageApi, CompactionMetrics metrics) {
        this.storageApi = storageApi;
        this.metrics = metrics;
    }

    public IEntryReader createEntryReader(String topic,
                                          long streamId,
                                          long startOffset,
                                          long endOffset,
                                          double avgEntrySize,
                                          EntryReaderOptions options) {
        // todo: pass the readMaxEntriesInOneBatch from the outside
        return new EntryReader(storageApi, streamId, startOffset, endOffset, 100, metrics);

    }

    @Override
    public void close() throws Exception {
        //  do nothing
    }
}
