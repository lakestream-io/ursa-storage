/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import io.oxia.client.api.AsyncOxiaClient;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Setter;

public abstract class FailureInjectedOxiaClient implements AsyncOxiaClient {

    @Setter
    protected volatile boolean failureMode = false;

    protected final Set<Long> failingStreamIds = ConcurrentHashMap.newKeySet();

    public void addFailingStreamId(long streamId) {
        failingStreamIds.add(streamId);
    }

    public void removeFailingStreamId(long streamId) {
        failingStreamIds.remove(streamId);
    }

    public void clearFailingStreamIds() {
        failingStreamIds.clear();
    }

    protected boolean shouldFailForKey(String key) {
        for (long streamId : failingStreamIds) {
            String prefix = String.format("%020d", streamId);
            if (key.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    protected static <T> CompletableFuture<T> failedFuture() {
        return CompletableFuture.failedFuture(new Exception("operation failed"));
    }

}
