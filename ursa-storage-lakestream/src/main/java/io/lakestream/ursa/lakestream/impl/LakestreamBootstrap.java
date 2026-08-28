/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import io.lakestream.api.LogStateManager;
import io.lakestream.api.LogStorage;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.impl.EntryIndexCache;
import io.lakestream.ursa.storage.impl.StorageApiLogStorage;

/**
 * Factory methods that bridge {@code ursa-storage-core} types to Lakestream API types.
 *
 * <p>This class keeps core-specific adapters contained in the Lakestream implementation module.
 */
public final class LakestreamBootstrap {

    private LakestreamBootstrap() {
    }

    /**
     * Creates a {@link LogStorage} adapter backed by the given {@link StorageApi}.
     */
    public static LogStorage createLogStorage(StorageApi storageApi) {
        return new StorageApiLogStorage(storageApi);
    }

    /**
     * Returns the {@link LogStateManager} from the given {@link StorageApi}.
     */
    public static LogStateManager createStateManager(StorageApi storageApi) {
        return storageApi.getStreamStateManager();
    }

    /**
     * Creates an {@link EntryIndexCache} backed by the given {@link StorageApi}.
     */
    public static EntryIndexCache createEntryIndexCache(StorageApi storageApi, int maxSize, int ttlInSecs) {
        return new EntryIndexCache(storageApi::readEntryIndex, maxSize, ttlInSecs);
    }
}
