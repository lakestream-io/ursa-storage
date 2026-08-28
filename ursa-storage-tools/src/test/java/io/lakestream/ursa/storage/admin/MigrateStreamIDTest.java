/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.lakestream.ursa.json.UrsaObjectMapperFactory;
import io.lakestream.ursa.storage.StreamProperties;
import io.lakestream.ursa.storage.UrsaStorage;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.storage.impl.StorageFormat;
import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.options.PutOption;
import io.oxia.testcontainers.OxiaContainer;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

public class MigrateStreamIDTest {

    private static final String OXIA_IMAGE = "oxia/oxia:main";

    private OxiaContainer oxiaContainer;

    @BeforeEach
    void setUp() throws Exception {
        oxiaContainer = new OxiaContainer(DockerImageName.parse(OXIA_IMAGE));
        oxiaContainer.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (oxiaContainer != null) {
            oxiaContainer.stop();
        }
    }

    @Test
    void shouldMigrateLegacyStreamIdEntries() throws Exception {
        final String key = "streams/test";
        final long streamId = 123L;
        final String generatorKey = StorageFormat.STREAM_ID_GENERATOR_PATH + "/" + key;
        final String registerKey = StorageFormat.STREAM_REGISTER_PATH + "/" + streamId;

        StorageConfig config = new StorageConfig();
        config.setBackendStorageType("local");
        config.setStoragePath("");
        config.setOxiaStorageUrl("oxia://" + oxiaContainer.getServiceAddress());

        try (UrsaStorage ursaStorage = new UrsaStorage(config, OpenTelemetry.noop())) {
            var storageApi = ursaStorage.getDefaultStorageApi();
            AsyncOxiaClient oxiaClient = storageApi.getStorageOxiaClient();

            oxiaClient.put(generatorKey,
                            Long.toString(streamId).getBytes(StandardCharsets.UTF_8),
                            Set.of(PutOption.PartitionKey(StorageFormat.STREAM_ID_GENERATOR_PATH)))
                    .join();

            oxiaClient.put(registerKey, new byte[0]).join();

            MigrateStreamID migrateStreamID = new MigrateStreamID();
            int result = migrateStreamID.execute(storageApi);
            assertEquals(0, result);

            GetResult resultGet = oxiaClient.get(registerKey).join();
            assertNotNull(resultGet);

            StreamProperties properties = UrsaObjectMapperFactory.getMapper()
                    .readValue(resultGet.value(), StreamProperties.class);
            assertEquals(key, properties.key());
        }
    }
}
