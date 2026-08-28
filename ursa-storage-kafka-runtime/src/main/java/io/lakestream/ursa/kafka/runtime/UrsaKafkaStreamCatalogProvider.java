/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.kafka.runtime;

import io.lakestream.api.ExternalStreamRegistry;
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamCatalogProvider;
import io.lakestream.ursa.kafka.reader.KafkaLakehouseReaderFactory;
import io.lakestream.ursa.lakestream.impl.DefaultCatalogPaths;
import io.lakestream.ursa.lakestream.impl.ExternalStreamRegistryService;
import io.lakestream.ursa.lakestream.impl.StreamCatalogService;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/** Ursa-owned runtime wiring for Kafka-backed Lakestream catalogs. */
public final class UrsaKafkaStreamCatalogProvider implements StreamCatalogProvider {

    private static final String SERVICE_NAME = "kafka-diskless-storage";
    private final StreamCatalogService streamCatalogService;
    private final ExternalStreamRegistryService externalStreamRegistryService;

    public UrsaKafkaStreamCatalogProvider() {
        this(new StreamCatalogService(), new ExternalStreamRegistryService());
    }

    UrsaKafkaStreamCatalogProvider(
            StreamCatalogService streamCatalogService,
            ExternalStreamRegistryService externalStreamRegistryService) {
        this.streamCatalogService = Objects.requireNonNull(
            streamCatalogService, "streamCatalogService");
        this.externalStreamRegistryService = Objects.requireNonNull(
            externalStreamRegistryService, "externalStreamRegistryService");
    }

    @Override
    public StreamCatalog open(String catalogMetadataUri, Properties properties) throws Exception {
        Properties runtimeProperties = prepareProperties(properties);
        OpenTelemetrySdk openTelemetrySdk = createOpenTelemetrySdk();
        return streamCatalogService.open(
            catalogMetadataUri,
            new DefaultCatalogPaths(),
            runtimeProperties,
            openTelemetrySdk,
            new KafkaLakehouseReaderFactory(),
            List.of(openTelemetrySdk));
    }

    @Override
    public ExternalStreamRegistry openExternalStreamRegistry(
            String catalogMetadataUri, Properties properties) throws Exception {
        OpenTelemetrySdk openTelemetrySdk = createOpenTelemetrySdk();
        return externalStreamRegistryService.open(
            catalogMetadataUri,
            new DefaultCatalogPaths(),
            properties,
            openTelemetrySdk,
            List.of(openTelemetrySdk));
    }

    static Properties prepareProperties(Properties source) {
        Properties properties = new Properties();
        properties.putAll(source);
        for (String name : source.stringPropertyNames()) {
            properties.setProperty(name, source.getProperty(name));
        }
        properties.putIfAbsent("storageTier", "default");

        if (!hasText(properties.getProperty("compactionBackendStorageType"))) {
            String backend = properties.getProperty("backendStorageType", "LOCAL")
                .toUpperCase(Locale.ROOT)
                .replace("_", "");
            properties.setProperty("compactionBackendStorageType",
                "AZUREBLOB".equals(backend) ? "AZUREDFS" : backend);
        }

        if (!hasText(properties.getProperty("compactionBucketRegion"))) {
            String region = firstNonBlank(properties.getProperty("region"),
                properties.getProperty("s3Region"));
            if (region != null) {
                properties.setProperty("compactionBucketRegion", region);
            }
        }
        return properties;
    }

    static Map<String, String> openTelemetryProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("otel.service.name", SERVICE_NAME);
        properties.put("otel.metrics.exporter", "none");
        properties.put("otel.traces.exporter", "none");
        properties.put("otel.logs.exporter", "none");
        for (String name : System.getProperties().stringPropertyNames()) {
            if (name.startsWith("otel.")) {
                properties.put(name, System.getProperty(name));
            }
        }
        return properties;
    }

    private static OpenTelemetrySdk createOpenTelemetrySdk() {
        return AutoConfiguredOpenTelemetrySdk.builder()
            .addPropertiesSupplier(UrsaKafkaStreamCatalogProvider::openTelemetryProperties)
            .build()
            .getOpenTelemetrySdk();
    }

    private static String firstNonBlank(String first, String second) {
        if (hasText(first)) {
            return first;
        }
        return hasText(second) ? second : null;
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
