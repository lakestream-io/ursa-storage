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

}
