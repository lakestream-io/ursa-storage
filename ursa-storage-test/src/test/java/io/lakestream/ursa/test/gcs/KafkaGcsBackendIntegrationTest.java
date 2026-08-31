/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.test.gcs;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.StorageOptions;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.impl.GCSFileStorage;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.test.KafkaBackendTestSupport;
import io.netty.buffer.ByteBuf;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Properties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Tag("docker")
@Testcontainers(disabledWithoutDocker = true)
class KafkaGcsBackendIntegrationTest {

    private static final String BUCKET = "kafka-ingestion";

    @Container
    static final GenericContainer<?> GCS = new GenericContainer<>("fsouza/fake-gcs-server")
        .withExposedPorts(4443)
        .withCreateContainerCmdModifier(command -> command.withEntrypoint(
            "/bin/fake-gcs-server", "-scheme", "http"));

    private static String endpoint;

    @BeforeAll
    static void prepareBucket() throws Exception {
        endpoint = "http://" + GCS.getHost() + ":" + GCS.getFirstMappedPort();
        updateExternalUrl(endpoint);
        var client = StorageOptions.newBuilder()
            .setHost(endpoint)
            .setProjectId("ursa-storage-test")
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getService();
        client.create(BucketInfo.newBuilder(BUCKET).build());
    }

    @Test
    void roundTripsKafkaMemoryRecords() throws Exception {
        var config = new StorageConfig();
        config.setBucket(BUCKET);
        config.setPrefix("records");
        config.setCloudStorageEndpoint(endpoint);
        var properties = new Properties();
        properties.setProperty("disableCredential", "true");
        config.setProperties(properties);

        try (var storage = new GCSFileStorage(config, InstrumentProvider.NOOP)) {
            ByteBuf payload = KafkaBackendTestSupport.payload();
            try {
                storage.put(payload, "orders/17");
            } finally {
                payload.release();
            }

            ByteBuf stored = storage.get("orders/17");
            try {
                KafkaBackendTestSupport.assertPayload(stored);
            } finally {
                stored.release();
            }
        }
    }

    private static void updateExternalUrl(String externalUrl) throws Exception {
        String requestJson = "{\"externalUrl\":\"" + externalUrl + "\"}";
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(externalUrl + "/_internal/config"))
            .header("Content-Type", "application/json")
            .PUT(HttpRequest.BodyPublishers.ofString(requestJson))
            .build();
        HttpResponse<Void> response = HttpClient.newHttpClient()
            .send(request, HttpResponse.BodyHandlers.discarding());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Failed to configure fake GCS server: " + response.statusCode());
        }
    }
}
