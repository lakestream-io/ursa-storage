/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.StorageFormat;
import io.lakestream.ursa.storage.impl.TestPersistStorageApi;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

@Slf4j
public class TestPersistStorageApiWithV3SerDe extends TestPersistStorageApi {


    @BeforeEach
    public void setup() throws Exception {
        var config = StorageConfig.builder()
                .indexSerializeFormatVersion(3)
                .backendStorageType("local").build();
        ursaStorageTestBase = new UrsaStorageTestBase();
        ursaStorageTestBase.setup(
            UrsaStorageTestBase.UrsaStorageTestConfig.builder()
                .ursaConfig(config)
                .build()
        );
        this.client = ursaStorageTestBase.getFailureInjectedOxiaClient();
        this.failureInjectedStorage = ursaStorageTestBase.getFailureInjectedStorage();
        StorageFormat format = new StorageFormat(config);
        this.storage = ursaStorageTestBase.createStorageApi(InstrumentProvider.NOOP);
    }

    @AfterEach
    public void cleanup() {
        ursaStorageTestBase.cleanup();
    }

}
