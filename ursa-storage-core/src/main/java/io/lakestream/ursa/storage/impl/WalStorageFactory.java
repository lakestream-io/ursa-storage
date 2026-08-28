/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import io.lakestream.api.LogStateManager;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.IDGenerator;
import io.lakestream.ursa.storage.WalStorage;
import io.netty.buffer.ByteBufAllocator;
import io.oxia.client.api.AsyncOxiaClient;
import java.io.IOException;

public class WalStorageFactory {

    public enum Type {
        SIMPLE,
    }

    public static WalStorage create(Type type, StorageConfig config, ByteBufAllocator allocator,
                                    FileStorage fileStorage,
                                    IDGenerator idGenerator, InstrumentProvider instrumentProvider,
                                    AsyncOxiaClient oxiaClient, StorageFormat format,
                                    LogStateManager streamStateManager) throws IOException {
        return switch (type) {
            case SIMPLE -> new ObjectWalStorageImpl(allocator, fileStorage, idGenerator, config, instrumentProvider,
                    oxiaClient, format, streamStateManager);
        };
    }
}
