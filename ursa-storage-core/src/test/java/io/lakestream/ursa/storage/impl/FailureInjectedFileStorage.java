/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.ursa.storage.FileStorage;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.Setter;

public class FailureInjectedFileStorage implements FileStorage {

    @Setter
    protected volatile boolean failureMode = false;

    protected static <T> CompletableFuture<T> failedFuture() {
        return CompletableFuture.failedFuture(new Exception("operation failed"));
    }

    private final FileStorage fileStorage;

    public FailureInjectedFileStorage(FileStorage fileStorage) {
        this.fileStorage = fileStorage;
    }

    @Override
    public void put(ByteBuf data, String location) throws IOException {
        if (failureMode) {
            throw new IOException("Failed to put data into " + location);
        }
        fileStorage.put(data, location);
    }

    @Override
    public CompletableFuture<Void> putAsync(ByteBuf data, String location) {
        if (failureMode) {
            return failedFuture();
        }
        return fileStorage.putAsync(data, location);
    }

    @Override
    public ByteBuf get(String location) throws IOException {
        if (failureMode) {
            throw new IOException("Failed to get data from the location " + location);
        }
        return fileStorage.get(location);
    }

    @Override
    public CompletableFuture<ByteBuf> getAsync(String location) {
        if (failureMode) {
            return CompletableFuture.failedFuture(new IOException("Failed to get data from the location " + location));
        }
        return fileStorage.getAsync(location);
    }

    @Override
    public void delete(String location) throws IOException {
        if (failureMode) {
            throw new IOException("Failed to delete data from the location " + location);
        }
        fileStorage.delete(location);
    }

    @Override
    public CompletableFuture<Void> deleteAsync(List<String> locations) {
        if (failureMode) {
            return CompletableFuture.failedFuture(new IOException("Failed to delete data from locations " + locations));
        }
        return fileStorage.deleteAsync(locations);
    }

    @Override
    public CompletableFuture<Void> deleteWithDatePrefixes(Set<String> prefixes) throws IOException {
        if (failureMode) {
            throw new IOException("Failed to execute deleteWithLifecycleUsingPrefixes ");
        }
        return fileStorage.deleteWithDatePrefixes(prefixes);
    }

    @Override
    public void close() throws Exception {
        fileStorage.close();
    }
}
