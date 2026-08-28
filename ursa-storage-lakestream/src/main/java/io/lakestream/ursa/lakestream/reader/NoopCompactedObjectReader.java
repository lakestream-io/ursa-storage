/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.reader;

import io.lakestream.ursa.storage.Entry;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class NoopCompactedObjectReader implements CompactedObjectReader {

    @Override
    public CompletableFuture<ReadResult> readMessagesAsync(String path, long startOffset, long baseOffset,
                                                      long maxNumOfMessages, long maxSize) {
        return CompletableFuture.failedFuture(new IOException("Not available because lakehouse reader is disabled"));
    }

    @Override
    public boolean hasSpaceInCache() {
        return false;
    }

    @Override
    public CompletableFuture<Entry> preFetchMessagesAsync(String path, long startOffset, long baseOffset,
                                                          long maxNumOfMessages, long maxSize, long estimatedSize) {
        return CompletableFuture.failedFuture(new IOException("Not available because lakehouse reader is disabled"));
    }

    @Override
    public void close() { }
}
