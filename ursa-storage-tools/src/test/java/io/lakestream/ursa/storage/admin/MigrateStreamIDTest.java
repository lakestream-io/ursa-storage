/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lakestream.ursa.json.UrsaObjectMapperFactory;
import io.lakestream.ursa.storage.StorageApi;
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
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

public class MigrateStreamIDTest {

    // Pinned rather than floating on :main so a server-side change to range-scan bound semantics
    // shows up as a deliberate version bump. Matches the tag used elsewhere in the repo.
    private static final String OXIA_IMAGE = "oxia/oxia:0.16.7";

    private static final Map<String, Long> STREAM_IDS_BY_KEY = Map.of(
            "test", 120L,
            "streams/test", 121L,
            "tenant/namespace/test", 122L,
            "public/default/persistent/test", 123L,
            "organization/team/namespace/category/stream", 124L);

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

    private StorageConfig newConfig() {
        StorageConfig config = new StorageConfig();
        config.setBackendStorageType("local");
        config.setStoragePath("");
        config.setOxiaStorageUrl("oxia://" + oxiaContainer.getServiceAddress());
        return config;
    }

    private void putGeneratorKey(AsyncOxiaClient oxiaClient, String key, byte[] value) {
        oxiaClient.put(StorageFormat.STREAM_ID_GENERATOR_PATH + "/" + key, value,
                Set.of(PutOption.PartitionKey(StorageFormat.STREAM_ID_GENERATOR_PATH))).join();
    }

    private void seedLegacyEntries(AsyncOxiaClient oxiaClient) {
        for (var entry : STREAM_IDS_BY_KEY.entrySet()) {
            putGeneratorKey(oxiaClient, entry.getKey(),
                    Long.toString(entry.getValue()).getBytes(StandardCharsets.UTF_8));
            oxiaClient.put(StorageFormat.STREAM_REGISTER_PATH + "/" + entry.getValue(), new byte[0]).join();
        }
        // Same shard, outside the stream-id prefix: must be skipped by the prefix filter.
        oxiaClient.put(
                "/unrelated/key",
                "not-a-stream-id".getBytes(StandardCharsets.UTF_8),
                Set.of(PutOption.PartitionKey(StorageFormat.STREAM_ID_GENERATOR_PATH))).join();
    }

    private void assertAllMigrated(AsyncOxiaClient oxiaClient) throws Exception {
        for (var entry : STREAM_IDS_BY_KEY.entrySet()) {
            var registerKey = StorageFormat.STREAM_REGISTER_PATH + "/" + entry.getValue();
            GetResult resultGet = oxiaClient.get(registerKey).join();
            assertNotNull(resultGet);

            StreamProperties properties = UrsaObjectMapperFactory.getMapper()
                    .readValue(resultGet.value(), StreamProperties.class);
            assertEquals(entry.getKey(), properties.key());
        }
    }

    @Test
    void shouldMigrateOpaqueStreamIdEntries() throws Exception {
        try (UrsaStorage ursaStorage = new UrsaStorage(newConfig(), OpenTelemetry.noop())) {
            StorageApi storageApi = ursaStorage.getDefaultStorageApi();
            AsyncOxiaClient oxiaClient = storageApi.getStorageOxiaClient();
            seedLegacyEntries(oxiaClient);

            assertEquals(0, new MigrateStreamID().execute(storageApi));

            assertAllMigrated(oxiaClient);
        }
    }

    @Test
    void shouldLeaveAlreadyMigratedEntriesUntouchedWhenRerun() throws Exception {
        try (UrsaStorage ursaStorage = new UrsaStorage(newConfig(), OpenTelemetry.noop())) {
            StorageApi storageApi = ursaStorage.getDefaultStorageApi();
            AsyncOxiaClient oxiaClient = storageApi.getStorageOxiaClient();
            seedLegacyEntries(oxiaClient);

            assertEquals(0, new MigrateStreamID().execute(storageApi));
            var versionsAfterFirstRun = STREAM_IDS_BY_KEY.values().stream()
                    .collect(Collectors.toMap(
                        streamId -> streamId,
                        streamId -> oxiaClient.get(StorageFormat.STREAM_REGISTER_PATH + "/" + streamId)
                                .join().version().versionId()));

            // Re-running a migration is a normal operator action after a partial failure.
            assertEquals(0, new MigrateStreamID().execute(storageApi));

            assertAllMigrated(oxiaClient);
            for (var entry : versionsAfterFirstRun.entrySet()) {
                var current = oxiaClient.get(StorageFormat.STREAM_REGISTER_PATH + "/" + entry.getKey()).join();
                assertEquals(entry.getValue(), current.version().versionId(),
                        "Re-running the migration must not rewrite stream ID " + entry.getKey());
            }
        }
    }

    @Test
    void shouldRejectMappingWithNonNumericStreamId() throws Exception {
        try (UrsaStorage ursaStorage = new UrsaStorage(newConfig(), OpenTelemetry.noop())) {
            StorageApi storageApi = ursaStorage.getDefaultStorageApi();
            AsyncOxiaClient oxiaClient = storageApi.getStorageOxiaClient();
            seedLegacyEntries(oxiaClient);
            // Under the prefix, so the prefix filter does not skip it: the value must fail parsing.
            putGeneratorKey(oxiaClient, "broken/stream", "not-a-number".getBytes(StandardCharsets.UTF_8));

            var error = assertThrows(CompletionException.class,
                    () -> new MigrateStreamID().execute(storageApi));

            var message = String.valueOf(error.getCause());
            assertTrue(message.contains(StorageFormat.STREAM_ID_GENERATOR_PATH + "/broken/stream"),
                    "Failure must name the offending key, but was: " + message);
        }
    }
}
