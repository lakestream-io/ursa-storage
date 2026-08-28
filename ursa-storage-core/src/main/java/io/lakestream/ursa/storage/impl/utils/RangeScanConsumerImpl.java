/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.utils;

import io.oxia.client.api.GetResult;
import io.oxia.client.api.RangeScanConsumer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;

public class RangeScanConsumerImpl implements RangeScanConsumer {
    final List<GetResult> results = new ArrayList<>();
    @Getter
    final CompletableFuture<List<GetResult>> future = new CompletableFuture<>();

    @Override
    public synchronized boolean onNext(GetResult getResult) {
        results.add(getResult);
        return true;
    }

    @Override
    public synchronized void onError(Throwable throwable) {
        future.completeExceptionally(throwable);
    }

    @Override
    public synchronized void onCompleted() {
        future.complete(results);
    }
}
