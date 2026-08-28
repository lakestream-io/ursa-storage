/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import java.util.Iterator;
import java.util.Objects;
import java.util.Properties;
import java.util.ServiceLoader;

/** Loads the single {@link StreamCatalogProvider} visible to a runtime class loader. */
public final class StreamCatalogLoader {

    private StreamCatalogLoader() {
    }

    /**
     * Opens a catalog using the thread context class loader.
     *
     * <p>If the thread context class loader is absent, the class loader that loaded this class is
     * used instead.
     */
    public static StreamCatalog open(String catalogMetadataUri, Properties properties) throws Exception {
        return open(catalogMetadataUri, properties, contextClassLoader());
    }

    /** Opens a catalog using exactly one provider visible to {@code classLoader}. */
    public static StreamCatalog open(String catalogMetadataUri, Properties properties,
                                     ClassLoader classLoader) throws Exception {
        Objects.requireNonNull(catalogMetadataUri, "catalogMetadataUri");
        Objects.requireNonNull(properties, "properties");
        Objects.requireNonNull(classLoader, "classLoader");

        StreamCatalogProvider provider = loadProvider(classLoader);
        StreamCatalog catalog = provider.open(catalogMetadataUri, copyProperties(properties));
        if (catalog == null) {
            throw new IllegalStateException(
                "StreamCatalogProvider " + provider.getClass().getName() + " returned null");
        }
        return catalog;
    }

    static ClassLoader contextClassLoader() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        return classLoader != null ? classLoader : StreamCatalogLoader.class.getClassLoader();
    }

    static StreamCatalogProvider loadProvider(ClassLoader classLoader) {
        Objects.requireNonNull(classLoader, "classLoader");
        Iterator<StreamCatalogProvider> providers = ServiceLoader.load(
            StreamCatalogProvider.class, classLoader).iterator();
        if (!providers.hasNext()) {
            throw new IllegalStateException("No StreamCatalogProvider found");
        }
        StreamCatalogProvider provider = providers.next();
        if (providers.hasNext()) {
            throw new IllegalStateException("Multiple StreamCatalogProvider implementations found");
        }
        return provider;
    }

    static Properties copyProperties(Properties properties) {
        Objects.requireNonNull(properties, "properties");
        Properties propertiesCopy = new Properties();
        propertiesCopy.putAll(properties);
        for (String name : properties.stringPropertyNames()) {
            propertiesCopy.setProperty(name, properties.getProperty(name));
        }
        return propertiesCopy;
    }
}
