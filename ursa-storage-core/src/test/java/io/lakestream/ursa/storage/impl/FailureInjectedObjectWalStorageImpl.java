/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.api.EntryIndex;
import io.lakestream.api.LogStateManager;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.EntryList;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.IDGenerator;
import io.lakestream.ursa.storage.impl.exception.RetryableException;
import io.netty.buffer.ByteBufAllocator;
import io.oxia.client.api.AsyncOxiaClient;

public class FailureInjectedObjectWalStorageImpl extends ObjectWalStorageImpl {

    private long requestProcessingDelayMs = 0;
    private volatile boolean stopProcessing = false;
    private volatile boolean isConvertPersistCacheToEntryListPaused = false;

    public FailureInjectedObjectWalStorageImpl(ByteBufAllocator allocator,
                                               FileStorage fileStorage,
                                               IDGenerator idGenerator,
                                               StorageConfig storageConfig,
                                               InstrumentProvider instrumentProvider,
                                               AsyncOxiaClient oxiaClient,
                                               StorageFormat format,
                                               LogStateManager streamStateManager) {
        super(allocator, fileStorage, idGenerator, storageConfig, instrumentProvider, oxiaClient, format,
                streamStateManager);
    }

    @Override
    protected void processSingleRequest() {
        while (stopProcessing) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        if (requestProcessingDelayMs > 0) {
            try {
                Thread.sleep(requestProcessingDelayMs);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        super.processSingleRequest();
    }

    @Override
    protected boolean convertPersistCacheToEntryList(PersistCache cache, EntryIndex index, EntryList entryList)
            throws RetryableException {
        while (isConvertPersistCacheToEntryListPaused) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        return super.convertPersistCacheToEntryList(cache, index, entryList);
    }

    public void pauseConvertPersistCacheToEntryList() {
        isConvertPersistCacheToEntryListPaused = true;
    }

    public void resumeConvertPersistCacheToEntryList() {
        isConvertPersistCacheToEntryListPaused = false;
    }

    public void injectRequestProcessingDelay(long delayMs) {
        this.requestProcessingDelayMs = delayMs;
    }

    public void cancelInjectRequestProcessingDelay() {
        this.requestProcessingDelayMs = 0;
    }

    void stopProcessing() {
        this.stopProcessing = true;
    }

    void continueProcessing() {
        this.stopProcessing = false;
    }
}
