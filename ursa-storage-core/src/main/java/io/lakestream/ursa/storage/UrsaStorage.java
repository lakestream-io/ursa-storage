/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.impl.PersistStorageApi;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.StorageFormat;
import io.lakestream.ursa.storage.impl.StreamStateManagerImpl;
import io.lakestream.ursa.storage.impl.WalStorageFactory;
import io.netty.buffer.PooledByteBufAllocator;
import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.Properties;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;

public class UrsaStorage implements AutoCloseable {

    @Getter
    private StorageApi defaultStorageApi;
    @Getter
    private WalStorage defaultWalStorage;

    @Getter
    private FileStorage fileStorage;

    private boolean internalCreatedOxia = false;
    @Getter
    private AsyncOxiaClient oxiaClient;
    private StorageFormat storageFormat;
    private InstrumentProvider instrumentProvider;
    public UrsaStorage(Properties properties, OpenTelemetry otel) throws Exception {
        this(StorageConfig.fromProperties(properties), otel);
    }

    public UrsaStorage(StorageConfig config, OpenTelemetry otel) throws Exception {
        this(config, otel, null);
    }

    public UrsaStorage(StorageConfig config, OpenTelemetry otel, AsyncOxiaClient client) throws Exception {
        if (client == null) {
            this.oxiaClient = createOxiaClient(config, otel);
        } else {
            this.oxiaClient = client;
        }
        this.instrumentProvider = new InstrumentProvider(otel);
        this.storageFormat = new StorageFormat(config);
        final var streamStateManager = new StreamStateManagerImpl();
        this.fileStorage = FileStorage.create(config, instrumentProvider);

        IDGenerator idGenerator = IDGenerator.create(config.getIdGeneratorType(), "wal", this.oxiaClient);
        this.defaultWalStorage =
            WalStorageFactory.create(WalStorageFactory.Type.SIMPLE, config, PooledByteBufAllocator.DEFAULT,
                fileStorage, idGenerator, instrumentProvider, this.oxiaClient, storageFormat,
                streamStateManager);
        this.defaultWalStorage.initialize();
        this.defaultStorageApi = new PersistStorageApi(config, this.oxiaClient,
            defaultWalStorage, instrumentProvider, storageFormat, streamStateManager);

    }

    AsyncOxiaClient createOxiaClient(StorageConfig config, OpenTelemetry otel) throws Exception {
        internalCreatedOxia = true;
        return OxiaClientFactory.create(config.getOxiaStorageUrl(), config.getOxiaStorageConfig(), otel);
    }

    public static Pair<String, String> validateOxiaUrl(String metadataURL) throws Exception {
        return OxiaClientFactory.validateOxiaUrl(metadataURL);
    }

    @Override
    public void close() throws Exception {
        if (defaultStorageApi != null) {
            defaultStorageApi.close();
        }
        if (defaultWalStorage != null) {
            defaultWalStorage.close().get();
        }
        if (fileStorage != null) {
            fileStorage.close();
        }
        if (internalCreatedOxia) {
            oxiaClient.close();
        }
    }
}
