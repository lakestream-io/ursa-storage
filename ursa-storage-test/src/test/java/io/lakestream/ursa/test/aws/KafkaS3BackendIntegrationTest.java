/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.test.aws;

import io.lakestream.ursa.metrics.InstrumentProvider;
import io.lakestream.ursa.storage.impl.S3FileStorage;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.lakestream.ursa.test.KafkaBackendTestSupport;
import io.lakestream.ursa.test.containers.util.S3Container;
import io.netty.buffer.ByteBuf;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Tag("docker")
@Testcontainers(disabledWithoutDocker = true)
class KafkaS3BackendIntegrationTest {

    private static final String BUCKET = "kafka-ingestion";

    @Container
    static final S3Container S3 = new S3Container(Optional.empty());

    @BeforeAll
    static void prepareBucket() {
        S3.prepareBucket(BUCKET);
    }

    @Test
    void roundTripsKafkaMemoryRecords() throws Exception {
        var config = new StorageConfig();
        config.setRegion(S3.getRegion());
        config.setBucket(BUCKET);
        config.setPrefix("records");
        config.setCloudStorageEndpoint(S3.endpoint().toString());
        config.setS3AccessKeyId(S3.getAccessKey());
        config.setS3SecretAccessKey(S3.getSecretKey());

        try (var storage = new S3FileStorage(config, InstrumentProvider.NOOP)) {
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
}
