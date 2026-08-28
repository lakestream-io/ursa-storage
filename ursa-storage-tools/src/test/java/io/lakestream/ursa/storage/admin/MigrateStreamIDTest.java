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
import java.util.Map;
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
    void shouldMigrateOpaqueStreamIdEntries() throws Exception {
        final Map<String, Long> streamIdsByKey = Map.of(
                "test", 120L,
                "streams/test", 121L,
                "tenant/namespace/test", 122L,
                "public/default/persistent/test", 123L,
                "organization/team/namespace/category/stream", 124L);

        StorageConfig config = new StorageConfig();
        config.setBackendStorageType("local");
        config.setStoragePath("");
        config.setOxiaStorageUrl("oxia://" + oxiaContainer.getServiceAddress());

        try (UrsaStorage ursaStorage = new UrsaStorage(config, OpenTelemetry.noop())) {
            var storageApi = ursaStorage.getDefaultStorageApi();
            AsyncOxiaClient oxiaClient = storageApi.getStorageOxiaClient();

            for (var entry : streamIdsByKey.entrySet()) {
                var generatorKey = StorageFormat.STREAM_ID_GENERATOR_PATH + "/" + entry.getKey();
                oxiaClient.put(generatorKey,
                                Long.toString(entry.getValue()).getBytes(StandardCharsets.UTF_8),
                                Set.of(PutOption.PartitionKey(StorageFormat.STREAM_ID_GENERATOR_PATH)))
                        .join();
                oxiaClient.put(StorageFormat.STREAM_REGISTER_PATH + "/" + entry.getValue(), new byte[0]).join();
            }
            oxiaClient.put(
                    "/unrelated/key",
                    "not-a-stream-id".getBytes(StandardCharsets.UTF_8),
                    Set.of(PutOption.PartitionKey(StorageFormat.STREAM_ID_GENERATOR_PATH))).join();

            MigrateStreamID migrateStreamID = new MigrateStreamID();
            int result = migrateStreamID.execute(storageApi);
            assertEquals(0, result);

            for (var entry : streamIdsByKey.entrySet()) {
                var registerKey = StorageFormat.STREAM_REGISTER_PATH + "/" + entry.getValue();
                GetResult resultGet = oxiaClient.get(registerKey).join();
                assertNotNull(resultGet);

                StreamProperties properties = UrsaObjectMapperFactory.getMapper()
                        .readValue(resultGet.value(), StreamProperties.class);
                assertEquals(entry.getKey(), properties.key());
            }
        }
    }
}
