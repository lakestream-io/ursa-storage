/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import java.util.Properties;

/**
 * Service-provider interface for bootstrapping a {@link StreamCatalog} implementation.
 *
 * <p>Providers are discovered through {@link java.util.ServiceLoader}. Implementations own all
 * resources that they create and transfer that ownership to the returned catalog.
 */
public interface StreamCatalogProvider {

    /**
     * Opens a stream catalog backed by the supplied metadata service.
     *
     * @param catalogMetadataUri metadata service URI
     * @param properties catalog and storage configuration
     * @return a non-null catalog that owns its implementation resources
     * @throws Exception if the catalog cannot be opened
     */
    StreamCatalog open(String catalogMetadataUri, Properties properties) throws Exception;

    /**
     * Opens a metadata-only registry for externally controlled streams.
     *
     * <p>Providers that support external control planes should override this method without
     * bootstrapping their data-plane storage runtime. The default keeps existing providers binary
     * compatible while failing explicitly instead of falling back to a full catalog.
     *
     * @param catalogMetadataUri metadata service URI
     * @param properties catalog metadata configuration
     * @return a non-null registry that owns its implementation resources
     * @throws Exception if the registry cannot be opened
     */
    default ExternalStreamRegistry openExternalStreamRegistry(
            String catalogMetadataUri, Properties properties) throws Exception {
        throw new UnsupportedOperationException(
            "External stream registration is not supported by " + getClass().getName());
    }
}
