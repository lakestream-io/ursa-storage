/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ExternalStreamRegistryLoaderTest {

    private static final ExternalStreamRegistry REGISTRY =
        (ExternalStreamRegistry) Proxy.newProxyInstance(
            ExternalStreamRegistryLoaderTest.class.getClassLoader(),
            new Class<?>[]{ExternalStreamRegistry.class},
            (proxy, method, args) -> null);
    private static final StreamCatalog CATALOG = (StreamCatalog) Proxy.newProxyInstance(
        ExternalStreamRegistryLoaderTest.class.getClassLoader(),
        new Class<?>[]{StreamCatalog.class},
        (proxy, method, args) -> null);

    @TempDir
    Path tempDir;

    @Test
    void shouldLoadMetadataRegistryAndCopyProperties() throws Exception {
        try (URLClassLoader loader = providerClassLoader(Provider.class)) {
            Properties defaults = new Properties();
            defaults.setProperty("inherited", "original-default");
            Properties properties = new Properties(defaults);
            properties.setProperty("key", "original");

            assertThat(ExternalStreamRegistryLoader.open(
                "oxia://localhost/test", properties, loader)).isSameAs(REGISTRY);

            properties.setProperty("key", "mutated");
            defaults.setProperty("inherited", "mutated-default");
            assertThat(Provider.lastProperties.getProperty("key")).isEqualTo("original");
            assertThat(Provider.lastProperties.getProperty("inherited"))
                .isEqualTo("original-default");
        }
    }

    @Test
    void shouldRejectProviderWithoutMetadataRegistrySupport() throws Exception {
        try (URLClassLoader loader = providerClassLoader(LegacyProvider.class)) {
            assertThatThrownBy(() -> ExternalStreamRegistryLoader.open(
                "oxia://localhost/test", new Properties(), loader))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("External stream registration is not supported");
        }
    }

    @Test
    void shouldRejectNullRegistry() throws Exception {
        try (URLClassLoader loader = providerClassLoader(NullRegistryProvider.class)) {
            assertThatThrownBy(() -> ExternalStreamRegistryLoader.open(
                "oxia://localhost/test", new Properties(), loader))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("returned null external stream registry");
        }
    }

    @Test
    void shouldUseSameSingleProviderValidationAsCatalogLoader() throws Exception {
        try (URLClassLoader loader = providerClassLoader(Provider.class, OtherProvider.class)) {
            assertThatThrownBy(() -> ExternalStreamRegistryLoader.open(
                "oxia://localhost/test", new Properties(), loader))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Multiple StreamCatalogProvider implementations found");
        }
    }

    @Test
    void legacyRegistryUsesUnsupportedPermanentDeletionDefault() {
        ExternalStreamRegistry registry = new LegacyExternalStreamRegistry();

        assertThatThrownBy(() -> registry.permanentlyDeleteExternalStream(
            new StreamIdentifier("namespace", "stream")))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Permanent external stream deletion is not supported");
    }

    private URLClassLoader providerClassLoader(Class<?>... providers) throws Exception {
        Path services = tempDir.resolve("META-INF/services");
        Files.createDirectories(services);
        String providerNames = Arrays.stream(providers)
            .map(Class::getName)
            .collect(Collectors.joining(System.lineSeparator()));
        Files.writeString(services.resolve(StreamCatalogProvider.class.getName()), providerNames);
        return new URLClassLoader(new URL[]{tempDir.toUri().toURL()}, getClass().getClassLoader());
    }

    public static class Provider implements StreamCatalogProvider {
        private static Properties lastProperties;

        @Override
        public StreamCatalog open(String catalogMetadataUri, Properties properties) {
            return CATALOG;
        }

        @Override
        public ExternalStreamRegistry openExternalStreamRegistry(
                String catalogMetadataUri, Properties properties) {
            lastProperties = properties;
            return REGISTRY;
        }
    }

    public static class OtherProvider extends Provider {
    }

    public static class LegacyProvider implements StreamCatalogProvider {
        @Override
        public StreamCatalog open(String catalogMetadataUri, Properties properties) {
            return CATALOG;
        }
    }

    public static class NullRegistryProvider extends Provider {
        @Override
        public ExternalStreamRegistry openExternalStreamRegistry(
                String catalogMetadataUri, Properties properties) {
            return null;
        }
    }

    private static final class LegacyExternalStreamRegistry implements ExternalStreamRegistry {

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
        public void close() {
        }
    }
}
