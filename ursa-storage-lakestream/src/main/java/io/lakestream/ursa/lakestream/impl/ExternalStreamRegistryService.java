/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import io.lakestream.api.CatalogPaths;
import io.lakestream.api.ExternalStreamRegistry;
import io.lakestream.ursa.storage.OxiaClientFactory;
import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/** Bootstraps an Oxia-only external stream registry without loading the storage data plane. */
public class ExternalStreamRegistryService {

    private static final String OXIA_STORAGE_CONFIG = "oxiaStorageConfig";

    @FunctionalInterface
    interface RegistryOxiaClientFactory {
        AsyncOxiaClient create(String oxiaUri, String configJson, OpenTelemetry otel)
                throws Exception;
    }

    private final RegistryOxiaClientFactory oxiaClientFactory;

    public ExternalStreamRegistryService() {
        this(OxiaClientFactory::create);
    }

    ExternalStreamRegistryService(RegistryOxiaClientFactory oxiaClientFactory) {
        this.oxiaClientFactory = Objects.requireNonNull(oxiaClientFactory, "oxiaClientFactory");
    }

    /** Opens a registry with default catalog paths and noop telemetry. */
    public ExternalStreamRegistry open(String oxiaUri, Properties properties) throws Exception {
        return open(oxiaUri, new DefaultCatalogPaths(), properties, OpenTelemetry.noop(), List.of());
    }

    /**
     * Opens a metadata-only registry.
     *
     * <p>The registry owns its Oxia client and every additional resource. If bootstrap fails, all
     * resources whose ownership was supplied to this method are closed before the failure is
     * propagated.
     */
    public ExternalStreamRegistry open(
            String oxiaUri, CatalogPaths catalogPaths, Properties properties,
            OpenTelemetry otel, List<? extends AutoCloseable> additionalOwnedResources)
            throws Exception {
        Objects.requireNonNull(oxiaUri, "oxiaUri");
        Objects.requireNonNull(catalogPaths, "catalogPaths");
        Objects.requireNonNull(properties, "properties");
        Objects.requireNonNull(otel, "otel");
        Objects.requireNonNull(additionalOwnedResources, "additionalOwnedResources");

        List<AutoCloseable> additionalOwned = new ArrayList<>();
        for (AutoCloseable resource : additionalOwnedResources) {
            additionalOwned.add(Objects.requireNonNull(
                resource, "additionalOwnedResources contains null"));
        }

        List<AutoCloseable> owned = new ArrayList<>();
        boolean additionalResourcesTransferred = false;
        try {
            String configJson = properties.getProperty(OXIA_STORAGE_CONFIG, "");
            AsyncOxiaClient oxiaClient = Objects.requireNonNull(
                oxiaClientFactory.create(oxiaUri, configJson, otel),
                "oxiaClientFactory returned null");
            owned.add(oxiaClient::close);
            owned.addAll(additionalOwned);
            additionalResourcesTransferred = true;
            return new IndexedExternalStreamRegistry(oxiaClient, catalogPaths, owned);
        } catch (Exception | Error failure) {
            closeAfterFailure(owned, failure);
            if (!additionalResourcesTransferred) {
                closeAfterFailure(additionalOwned, failure);
            }
            throw failure;
        }
    }

    private static void closeAfterFailure(
            List<? extends AutoCloseable> resources, Throwable failure) {
        for (AutoCloseable resource : resources) {
            try {
                resource.close();
            } catch (Exception | Error closeFailure) {
                if (closeFailure != failure) {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
    }
}
