/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.Notification;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.RangeScanConsumer;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.DeleteRangeOption;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.GetSequenceUpdatesOption;
import io.oxia.client.api.options.ListOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.RangeScanOption;
import java.io.Closeable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import lombok.NonNull;

public class RealOxiaClient extends FailureInjectedOxiaClient {

    private final AsyncOxiaClient client;

    public RealOxiaClient(AsyncOxiaClient client) {
        this.client = client;
    }

    @Override
    public @NonNull CompletableFuture<PutResult> put(String key, byte[] value, Set<PutOption> options) {
        if (failureMode || shouldFailForKey(key)) {
            return failedFuture();
        }
        return client.put(key, value, options);
    }

    @Override
    public @NonNull CompletableFuture<PutResult> put(String key, byte[] value) {
        if (failureMode || shouldFailForKey(key)) {
            return failedFuture();
        }
        return client.put(key, value);
    }

    @Override
    public @NonNull CompletableFuture<Boolean> delete(String key, Set<DeleteOption> options) {
        if (failureMode) {
            return failedFuture();
        }
        return client.delete(key, options);
    }

    @Override
    public @NonNull CompletableFuture<Boolean> delete(String key) {
        if (failureMode) {
            return failedFuture();
        }
        return client.delete(key);
    }

    @Override
    public @NonNull CompletableFuture<Void> deleteRange(String startKeyInclusive, String endKeyExclusive) {
        if (failureMode) {
            return failedFuture();
        }
        return client.deleteRange(startKeyInclusive, endKeyExclusive);
    }

    @Override
    public @NonNull CompletableFuture<Void> deleteRange(String startKeyInclusive,
            String endKeyExclusive, Set<DeleteRangeOption> options) {
        if (failureMode) {
            return failedFuture();
        }
        return client.deleteRange(startKeyInclusive, endKeyExclusive, options);
    }

    @Override
    public @NonNull CompletableFuture<GetResult> get(String key) {
        if (failureMode) {
            return failedFuture();
        }
        return client.get(key);
    }

    @Override
    public @NonNull CompletableFuture<GetResult> get(String key, Set<GetOption> options) {
        if (failureMode) {
            return failedFuture();
        }
        return client.get(key, options);
    }

    @Override
    public @NonNull CompletableFuture<List<String>> list(String startKeyInclusive, String endKeyExclusive) {
        if (failureMode) {
            return failedFuture();
        }
        return client.list(startKeyInclusive, endKeyExclusive);
    }

    @Override
    public @NonNull CompletableFuture<List<String>> list(String startKeyInclusive,
            String endKeyExclusive, Set<ListOption> options) {
        if (failureMode) {
            return failedFuture();
        }
        return client.list(startKeyInclusive, endKeyExclusive, options);
    }

    @Override
    public void rangeScan(@NonNull String startKeyInclusive,
            @NonNull String endKeyExclusive, @NonNull RangeScanConsumer consumer) {
        if (failureMode) {
            consumer.onError(new Exception("failed scan"));
        }
        client.rangeScan(startKeyInclusive, endKeyExclusive, consumer);
    }

    @Override
    public void rangeScan(@NonNull String startKeyInclusive,
            @NonNull String endKeyExclusive, @NonNull RangeScanConsumer consumer,
            @NonNull Set<RangeScanOption> options) {
        if (failureMode) {
            consumer.onError(new Exception("failed scan"));
        }
        client.rangeScan(startKeyInclusive, endKeyExclusive, consumer, options);
    }

    @Override
    public void notifications(@NonNull Consumer<Notification> notificationCallback) {
        client.notifications(notificationCallback);
    }

    @Override
    public Closeable getSequenceUpdates(@NonNull String s, @NonNull Consumer<String> consumer,
                                        @NonNull Set<GetSequenceUpdatesOption> set) {
        return client.getSequenceUpdates(s, consumer, set);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
