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
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StreamCatalogLoaderTest {

    private static final StreamCatalog CATALOG = (StreamCatalog) Proxy.newProxyInstance(
        StreamCatalogLoaderTest.class.getClassLoader(),
        new Class<?>[]{StreamCatalog.class},
        (proxy, method, args) -> null);

    @TempDir
    Path tempDir;

    @Test
    void shouldLoadExactlyOneProviderFromExplicitClassLoader() throws Exception {
        try (URLClassLoader loader = providerClassLoader(Provider.class)) {
            Properties defaults = new Properties();
            defaults.setProperty("inheritedKey", "inheritedOriginal");
            Properties properties = new Properties(defaults);
            properties.setProperty("key", "original");
            assertThat(StreamCatalogLoader.open("oxia://localhost/test", properties, loader))
                .isSameAs(CATALOG);
            properties.setProperty("key", "mutated");
            defaults.setProperty("inheritedKey", "inheritedMutated");
            assertThat(Provider.lastProperties.getProperty("key")).isEqualTo("original");
            assertThat(Provider.lastProperties.getProperty("inheritedKey"))
                .isEqualTo("inheritedOriginal");
        }
    }

    @Test
    void shouldUseThreadContextClassLoader() throws Exception {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader loader = providerClassLoader(Provider.class)) {
            Thread.currentThread().setContextClassLoader(loader);
            assertThat(StreamCatalogLoader.open("oxia://localhost/test", new Properties()))
                .isSameAs(CATALOG);
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    void shouldRejectMissingProvider() throws Exception {
        try (URLClassLoader loader = new URLClassLoader(new URL[]{tempDir.toUri().toURL()},
                getClass().getClassLoader())) {
            assertThatThrownBy(() -> StreamCatalogLoader.open(
                "oxia://localhost/test", new Properties(), loader))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("No StreamCatalogProvider found");
        }
    }

    @Test
    void shouldRejectMultipleProviders() throws Exception {
        try (URLClassLoader loader = providerClassLoader(Provider.class, OtherProvider.class)) {
            assertThatThrownBy(() -> StreamCatalogLoader.open(
                "oxia://localhost/test", new Properties(), loader))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Multiple StreamCatalogProvider implementations found");
        }
    }

    @Test
    void shouldRejectNullCatalogFromProvider() throws Exception {
        try (URLClassLoader loader = providerClassLoader(NullProvider.class)) {
            assertThatThrownBy(() -> StreamCatalogLoader.open(
                "oxia://localhost/test", new Properties(), loader))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("returned null");
        }
    }

    @Test
    void shouldRejectNullArguments() {
        assertThatThrownBy(() -> StreamCatalogLoader.open(null, new Properties(), getClass().getClassLoader()))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("catalogMetadataUri");
        assertThatThrownBy(() -> StreamCatalogLoader.open(
            "oxia://localhost/test", null, getClass().getClassLoader()))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("properties");
        assertThatThrownBy(() -> StreamCatalogLoader.open(
            "oxia://localhost/test", new Properties(), null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("classLoader");
    }

    private URLClassLoader providerClassLoader(Class<?>... providers) throws Exception {
        Path services = tempDir.resolve("META-INF/services");
        Files.createDirectories(services);
        String providerNames = java.util.Arrays.stream(providers)
            .map(Class::getName)
            .collect(java.util.stream.Collectors.joining(System.lineSeparator()));
        Files.writeString(services.resolve(StreamCatalogProvider.class.getName()), providerNames);
        return new URLClassLoader(new URL[]{tempDir.toUri().toURL()}, getClass().getClassLoader());
    }

    public static class Provider implements StreamCatalogProvider {
        private static Properties lastProperties;

        @Override
        public StreamCatalog open(String catalogMetadataUri, Properties properties) {
            lastProperties = properties;
            return CATALOG;
        }
    }

    public static class OtherProvider extends Provider {
    }

    public static class NullProvider implements StreamCatalogProvider {
        @Override
        public StreamCatalog open(String catalogMetadataUri, Properties properties) {
            return null;
        }
    }
}
