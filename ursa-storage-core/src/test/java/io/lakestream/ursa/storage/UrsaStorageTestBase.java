/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import io.lakestream.api.LogStateManager;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.impl.FailureInjectedFileStorage;
import io.lakestream.ursa.storage.impl.PersistStorageApi;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.StorageFormat;
import io.lakestream.ursa.storage.impl.StreamStateManagerImpl;
import io.lakestream.ursa.storage.impl.WalStorageFactory;
import io.netty.buffer.PooledByteBufAllocator;
import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.testcontainers.OxiaContainer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@Getter
public class UrsaStorageTestBase {
    public static final String OXIA_IMAGE = "oxia/oxia:main";

    private final LogStateManager streamStateManager = new StreamStateManagerImpl();
    private S3BasedTestClass s3BasedTestClass;
    private FileBasedTestClass fileBasedTestClass;
    private UrsaStorageTestConfig config;
    private OxiaContainer oxiaContainer;
    private FailureInjectedOxiaClient failureInjectedOxiaClient;
    private FailureInjectedFileStorage failureInjectedFileStorage;
    private FailureInjectedStorage failureInjectedStorage;
    private StorageFormat storageFormat;

    public void setup() throws Exception {
        setup(new UrsaStorageTestConfig());
    }

    public void setup(UrsaStorageTestConfig config) throws Exception {
        this.config = config;
        this.storageFormat = new StorageFormat(this.config.getUrsaConfig());
        switch (FileStorage.Type.valueOf(config.getUrsaConfig().getBackendStorageType().toUpperCase())) {
            case S3 -> setupS3();
            case LOCAL -> setupLocalFile();
        }
        this.oxiaContainer = new OxiaContainer(DockerImageName.parse(OXIA_IMAGE));
        this.oxiaContainer.setCommand("bin/oxia", "standalone", "-s", String.valueOf(config.getOxiaShards()));
        this.oxiaContainer.start();
        this.failureInjectedOxiaClient = new RealOxiaClient(getOxiaClient());
        this.failureInjectedFileStorage = new FailureInjectedFileStorage(getFileStorage());
        this.failureInjectedStorage = new FailureInjectedStorage(getWalStorage());
        this.failureInjectedStorage.initialize();
    }

    private void setupS3() throws Exception {
        this.s3BasedTestClass = new S3BasedTestClass();
        this.s3BasedTestClass.setup();
        StorageConfig ursaStorageConfig = this.config.getUrsaConfig();
        ursaStorageConfig.setCloudStorageEndpoint(this.s3BasedTestClass.localStack.getEndpoint().toString());
        ursaStorageConfig.setS3Region(this.s3BasedTestClass.localStack.getRegion());
        ursaStorageConfig.setS3Bucket(this.s3BasedTestClass.bucket);
        ursaStorageConfig.setS3AccessKeyId(this.s3BasedTestClass.localStack.getAccessKey());
        ursaStorageConfig.setS3SecretAccessKey(this.s3BasedTestClass.localStack.getSecretKey());
    }

    private void setupLocalFile() throws Exception {
        this.fileBasedTestClass = new FileBasedTestClass();
        this.fileBasedTestClass.setup();
        StorageConfig ursaStorageConfig = this.config.getUrsaConfig();
        ursaStorageConfig.setStoragePath(this.fileBasedTestClass.path.toString());
    }

    public void cleanup() {
        if (this.failureInjectedStorage != null) {
            try {
                this.failureInjectedStorage.close();
            } catch (Exception e) {
                log.error("Failed to close storage", e);
            }
        }
        if (this.failureInjectedFileStorage != null) {
            try {
                this.failureInjectedFileStorage.close();
            } catch (Exception e) {
                log.error("Failed to close file storage", e);
            }
        }
        if (this.failureInjectedOxiaClient != null) {
            try {
                this.failureInjectedOxiaClient.close();
            } catch (Exception e) {
                log.error("Failed to close oxia client", e);
            }
        }
        if (this.oxiaContainer != null) {
            try {
                this.oxiaContainer.stop();
            } catch (Exception e) {
                log.error("Failed to stop oxia container", e);
            }
        }
        if (this.s3BasedTestClass != null) {
            try {
                this.s3BasedTestClass.cleanup();
            } catch (Exception e) {
                log.error("Failed to cleanup s3 based test class", e);
            }
        }
        if (this.fileBasedTestClass != null) {
            try {
                this.fileBasedTestClass.cleanup();
            } catch (Exception e) {
                log.error("Failed to cleanup file based test class", e);
            }
        }
    }

    protected AsyncOxiaClient getOxiaClient() throws Exception{
        return OxiaClientBuilder.create(oxiaContainer.getServiceAddress())
            .openTelemetry(config.getOpenTelemetry())
            .asyncClient().get();
    }

    protected FileStorage getFileStorage() throws Exception {
        return FileStorage.create(config.getUrsaConfig(), config.getInstrumentProvider());
    }

    protected WalStorage getWalStorage() throws Exception {
        return WalStorageFactory.create(WalStorageFactory.Type.SIMPLE, config.getUrsaConfig(),
                PooledByteBufAllocator.DEFAULT,
                failureInjectedFileStorage, getIDGenerator(), config.getInstrumentProvider(), getOxiaClient(),
                storageFormat, streamStateManager);
    }

    protected IDGenerator getIDGenerator() throws Exception {
        return IDGenerator.create(config.getUrsaConfig().getIdGeneratorType(), "ursa-storage-test",
            failureInjectedOxiaClient);
    }

    public PersistStorageApi createStorageApi(InstrumentProvider instrumentProvider) {
        return new PersistStorageApi(config.getUrsaConfig(), failureInjectedOxiaClient, failureInjectedStorage,
                instrumentProvider, new StorageFormat(config.getUrsaConfig()), streamStateManager);
    }

    @Builder(toBuilder = true)
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class UrsaStorageTestConfig {
        @Builder.Default
        private StorageConfig ursaConfig = StorageConfig.builder()
            .writeBufferSize(1024 * 1024)
            .writeBufferSegment(4)
            .writeCacheEnabled(false)
            .build();
        @Builder.Default
        private InstrumentProvider instrumentProvider = InstrumentProvider.NOOP;
        @Builder.Default
        private OpenTelemetry openTelemetry = OpenTelemetry.noop();
        @Builder.Default
        private int oxiaShards = 32;
    }
}
