/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import io.lakestream.api.EntryIndex;
import io.lakestream.api.Position;
import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.Setter;

public class FailureInjectedStorage implements WalStorage {

    @Getter
    private final WalStorage storage;

    @Setter
    protected volatile boolean failureMode = false;

    @Setter
    protected volatile boolean partReadFailureMode = false;

    private final Random random = new Random();

    public FailureInjectedStorage(WalStorage storage) {
        this.storage = storage;
    }


    @Override
    public void initialize() throws Exception {
        storage.initialize();
    }

    @Override
    public CompletableFuture<AddResult> put(long id, int numberOfMessages, ByteBuf buf) {
        if (failureMode) {
            return failedFuture();
        }
        return storage.put(id, numberOfMessages, buf);
    }

    @Override
    public CompletableFuture<AddResult> put(long id, int numberOfMessages, long initialOffset, long cumulativeSize,
                                            ByteBuf buf) {
        if (failureMode) {
            return failedFuture();
        }
        return storage.put(id, numberOfMessages, initialOffset, cumulativeSize, buf);
    }

    @Override
    public CompletableFuture<AddResult> put(long id, ByteBuf buf) {
        if (failureMode) {
            return failedFuture();
        }
        return storage.put(id, buf);
    }

    @Override
    public CompletableFuture<Entry> get(long id, long offset, EntryIndex index) {
        if (failureMode) {
            return failedFuture();
        }
        return storage.get(id, offset, index);
    }

    @Override
    public CompletableFuture<Entry> get(long id, EntryIndex index) {
        if (failureMode) {
            return failedFuture();
        }
        return storage.get(id, index);
    }

    @Override
    public CompletableFuture<Void> get(List<EntryIndex> indices, EntryList entryList) {
        if (failureMode && !partReadFailureMode) {
            return failedFuture();
        }

        return storage.get(indices, entryList);
    }

    @Override
    public void preFetch(long id, List<Position> positions) {
        if (failureMode) {
            throw new RuntimeException("operation failed");
        }
        storage.preFetch(id, positions);
    }

    @Override
    public CompletableFuture<Void> delete(long id, List<Position> positions) {
        if (failureMode) {
            return failedFuture();
        }
        return storage.delete(id, positions);
    }

    private static <T> CompletableFuture<T> failedFuture() {
        return CompletableFuture.failedFuture(new Exception("operation failed"));
    }

    @Override
    public FileStorage getFileStorage() {
        return storage.getFileStorage();
    }

    @Override
    public CompletableFuture<Void> close() {
        return storage.close();
    }
}
