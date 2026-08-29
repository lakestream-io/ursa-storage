/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.api.ExternalStreamRegistry;
import io.lakestream.api.StreamIdentifier;
import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.options.PutOption;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ExternalStreamRegistryServiceTest {

    private static final Version VERSION =
        new Version(1, 0, 0, 0, Optional.empty(), Optional.empty());

    @Test
    void opensOnlyOxiaMetadataRegistryAndTransfersResourceOwnership() throws Exception {
        AsyncOxiaClient oxiaClient = mock(AsyncOxiaClient.class);
        AutoCloseable telemetry = mock(AutoCloseable.class);
        AtomicReference<String> capturedUri = new AtomicReference<>();
        AtomicReference<String> capturedConfig = new AtomicReference<>();
        ExternalStreamRegistryService service = new ExternalStreamRegistryService(
            (uri, configJson, otel) -> {
                capturedUri.set(uri);
                capturedConfig.set(configJson);
                return oxiaClient;
            });
        Properties properties = new Properties();
        properties.setProperty("oxiaStorageConfig", "{\"requestTimeout\":\"5s\"}");
        properties.setProperty("backendStorageType", "this-is-never-parsed");
        String configPath = "/admin/streams/public/default/topic";
        AtomicReference<byte[]> storedConfig = new AtomicReference<>();
        when(oxiaClient.get(configPath)).thenAnswer(ignored ->
            CompletableFuture.completedFuture(storedConfig.get() == null ? null
                : new GetResult(configPath, storedConfig.get(), VERSION)));
        when(oxiaClient.put(eq(configPath), any(byte[].class),
                eq(Set.of(PutOption.IfRecordDoesNotExist))))
            .thenAnswer(invocation -> {
                storedConfig.set(invocation.getArgument(1, byte[].class));
                return CompletableFuture.completedFuture(new PutResult(configPath, VERSION));
            });
        when(oxiaClient.put(eq(configPath), any(byte[].class),
                eq(Set.of(PutOption.IfVersionIdEquals(VERSION.versionId())))))
            .thenAnswer(invocation -> {
                storedConfig.set(invocation.getArgument(1, byte[].class));
                return CompletableFuture.completedFuture(new PutResult(configPath, VERSION));
            });

        ExternalStreamRegistry registry = service.open(
            "oxia://localhost/catalog", new DefaultCatalogPaths(), properties,
            OpenTelemetry.noop(), List.of(telemetry));
        registry.registerExternalStream(
            new StreamIdentifier("public/default", "topic"), 2, Map.of()).get();

        assertThat(capturedUri.get()).isEqualTo("oxia://localhost/catalog");
        assertThat(capturedConfig.get()).isEqualTo("{\"requestTimeout\":\"5s\"}");
        verify(oxiaClient).put(eq(configPath), any(byte[].class),
            eq(Set.of(PutOption.IfRecordDoesNotExist)));
        verify(oxiaClient).put(eq(configPath), any(byte[].class),
            eq(Set.of(PutOption.IfVersionIdEquals(VERSION.versionId()))));

        registry.close();
        registry.close();
        verify(oxiaClient, times(1)).close();
        verify(telemetry, times(1)).close();
    }

    @Test
    void closesAdditionalResourcesWhenOxiaBootstrapFails() throws Exception {
        AutoCloseable telemetry = mock(AutoCloseable.class);
        IllegalStateException failure = new IllegalStateException("oxia unavailable");
        ExternalStreamRegistryService service = new ExternalStreamRegistryService(
            (uri, configJson, otel) -> {
                throw failure;
            });

        assertThatThrownBy(() -> service.open(
            "oxia://localhost/catalog", new DefaultCatalogPaths(), new Properties(),
            OpenTelemetry.noop(), List.of(telemetry)))
            .isSameAs(failure);
        verify(telemetry).close();
    }
}
