/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import java.util.Objects;
import java.util.Properties;

/** Loads a metadata-only external stream registry from the runtime catalog provider. */
public final class ExternalStreamRegistryLoader {

    private ExternalStreamRegistryLoader() {
    }

    /** Opens a registry using the thread context class loader. */
    public static ExternalStreamRegistry open(
            String catalogMetadataUri, Properties properties) throws Exception {
        return open(catalogMetadataUri, properties, StreamCatalogLoader.contextClassLoader());
    }

    /** Opens a registry using exactly one catalog provider visible to {@code classLoader}. */
    public static ExternalStreamRegistry open(
            String catalogMetadataUri, Properties properties, ClassLoader classLoader)
            throws Exception {
        Objects.requireNonNull(catalogMetadataUri, "catalogMetadataUri");
        Objects.requireNonNull(properties, "properties");
        Objects.requireNonNull(classLoader, "classLoader");

        StreamCatalogProvider provider = StreamCatalogLoader.loadProvider(classLoader);
        ExternalStreamRegistry registry = provider.openExternalStreamRegistry(
            catalogMetadataUri, StreamCatalogLoader.copyProperties(properties));
        if (registry == null) {
            throw new IllegalStateException(
                "StreamCatalogProvider " + provider.getClass().getName()
                    + " returned null external stream registry");
        }
        return registry;
    }
}
