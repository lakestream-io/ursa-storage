/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.kafka.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.lakestream.api.CatalogPaths;
import io.lakestream.api.ExternalStreamRegistry;
import io.lakestream.api.StreamCatalogProvider;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.ursa.lakestream.impl.ExternalStreamRegistryService;
import io.lakestream.ursa.lakestream.impl.StreamCatalogService;
import io.opentelemetry.api.OpenTelemetry;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class UrsaKafkaStreamCatalogProviderTest {

    @Test
    void shouldRegisterExactlyOneProvider() {
        assertThat(ServiceLoader.load(StreamCatalogProvider.class).stream()
            .map(ServiceLoader.Provider::type))
            .containsExactly(UrsaKafkaStreamCatalogProvider.class);
    }

    @Test
    void shouldOpenExternalRegistryThroughMetadataOnlyService() throws Exception {
        AtomicReference<Properties> capturedProperties = new AtomicReference<>();
        ExternalStreamRegistry expected = new ExternalStreamRegistry() {
            @Override
            public CompletableFuture<Void> registerExternalStream(
                    StreamIdentifier id, int partitionCount, Map<String, String> properties) {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> unregisterExternalStream(StreamIdentifier id) {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> permanentlyDeleteExternalStream(StreamIdentifier id) {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public void close() {
            }
        };
        ExternalStreamRegistryService registryService = new ExternalStreamRegistryService() {
            @Override
            public ExternalStreamRegistry open(
                    String oxiaUri, CatalogPaths catalogPaths, Properties properties,
                    OpenTelemetry otel, List<? extends AutoCloseable> additionalOwnedResources)
                    throws Exception {
                capturedProperties.set(properties);
                for (AutoCloseable resource : additionalOwnedResources) {
                    resource.close();
                }
                return expected;
            }
        };
        UrsaKafkaStreamCatalogProvider provider = new UrsaKafkaStreamCatalogProvider(
            new StreamCatalogService(), registryService);
        Properties properties = new Properties();
        properties.setProperty("backendStorageType", "must-not-bootstrap-storage");

        assertThat(provider.openExternalStreamRegistry("oxia://localhost/catalog", properties))
            .isSameAs(expected);
        assertThat(capturedProperties.get()).isSameAs(properties);
    }

    @Test
    void runtimeClasspathMustNotContainApacheKafkaArtifacts() {
        assertThatThrownBy(() -> Class.forName(
            "org.apache.kafka.common.TopicPartition", false, getClass().getClassLoader()))
            .isInstanceOf(ClassNotFoundException.class);
    }

    @Test
    void shouldDeriveKafkaReaderPropertiesWithoutMutatingCaller() {
        Properties source = new Properties();
        source.setProperty("backendStorageType", "Azure_Blob");
        source.setProperty("region", "us-west-2");

        Properties prepared = UrsaKafkaStreamCatalogProvider.prepareProperties(source);

        assertThat(prepared)
            .containsEntry("storageTier", "default")
            .containsEntry("compactionBackendStorageType", "AZUREDFS")
            .containsEntry("compactionBucketRegion", "us-west-2");
        assertThat(source)
            .doesNotContainKeys("compactionBackendStorageType", "compactionBucketRegion");
    }

    @Test
    void shouldPreserveExplicitReaderPropertiesAndFallBackToS3Region() {
        Properties source = new Properties();
        source.setProperty("backendStorageType", "S3");
        source.setProperty("compactionBackendStorageType", "GCS");
        source.setProperty("compactionBucketRegion", "explicit-region");
        source.setProperty("region", "generic-region");
        source.setProperty("s3Region", "s3-region");

        assertThat(UrsaKafkaStreamCatalogProvider.prepareProperties(source))
            .containsEntry("compactionBackendStorageType", "GCS")
            .containsEntry("compactionBucketRegion", "explicit-region");

        source.remove("compactionBucketRegion");
        source.remove("region");
        assertThat(UrsaKafkaStreamCatalogProvider.prepareProperties(source))
            .containsEntry("compactionBucketRegion", "s3-region");
    }

    @Test
    void shouldApplySafeTelemetryDefaults() {
        assertThat(UrsaKafkaStreamCatalogProvider.openTelemetryProperties())
            .containsEntry("otel.service.name", "kafka-diskless-storage")
            .containsEntry("otel.metrics.exporter", "none")
            .containsEntry("otel.traces.exporter", "none")
            .containsEntry("otel.logs.exporter", "none");
    }
}
