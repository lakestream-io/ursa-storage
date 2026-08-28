/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.Authentication;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class OxiaClientFactoryTest {

    @Test
    void testValidateOxiaUrl() {
        assertEquals("127.0.0.1:6648", OxiaClientFactory.validateOxiaUrl("oxia://127.0.0.1:6648/ns").getLeft());
        assertEquals("ns", OxiaClientFactory.validateOxiaUrl("oxia://127.0.0.1:6648/ns").getRight());
        assertEquals("default", OxiaClientFactory.validateOxiaUrl("oxia://127.0.0.1:6648").getRight());
        assertEquals("127.0.0.1:6648", OxiaClientFactory.validateOxiaUrl("127.0.0.1:6648/ns").getLeft());
        assertEquals("ns", OxiaClientFactory.validateOxiaUrl("127.0.0.1:6648/ns").getRight());
        assertEquals("default", OxiaClientFactory.validateOxiaUrl("127.0.0.1:6648").getRight());
    }

    @Test
    void testValidateOxiaUrlInvalid() {
        assertThrows(IllegalArgumentException.class, () -> OxiaClientFactory.validateOxiaUrl(null));
        assertThrows(IllegalArgumentException.class, () -> OxiaClientFactory.validateOxiaUrl(""));
        assertThrows(IllegalArgumentException.class, () -> OxiaClientFactory.validateOxiaUrl("zk://127.0.0.1:2181"));
        assertThrows(IllegalArgumentException.class, () -> OxiaClientFactory.validateOxiaUrl(
                "oxia://127.0.0.1:6648/ns/extra"));
    }

    @Test
    void testCreateBuilderLoadsJsonConfigAndAllowsNamespaceOverride() throws Exception {
        String configJson = """
                {
                  "namespace": "config-namespace",
                  "enableTls": "true",
                  "authPluginClassName": "io.oxia.client.auth.TokenAuthentication",
                  "authParams": "token:abc",
                  "maxRequestsPerBatch": "123",
                  "requestTimeout": "5000"
                }
                """;

        var builder = OxiaClientFactory.createBuilder("oxia://127.0.0.1:6648/storage", configJson,
                OpenTelemetry.noop());
        Object clientConfig = getClientConfig(builder);

        assertEquals("127.0.0.1:6648", invoke(clientConfig, "serviceAddress"));
        assertEquals("config-namespace", invoke(clientConfig, "namespace"));
        assertTrue((Boolean) invoke(clientConfig, "enableTls"));
        assertEquals(123, invoke(clientConfig, "maxRequestsPerBatch"));
        assertEquals(Duration.ofMillis(5000), invoke(clientConfig, "requestTimeout"));

        Authentication authentication = (Authentication) invoke(clientConfig, "authentication");
        assertNotNull(authentication);
        assertEquals("Bearer abc", authentication.generateCredentials().get("Authorization"));
    }

    @Test
    void testOxiaBuilderProviderLoadsFromRuntimeClasspath() throws Exception {
        String testClassPath = System.getProperty(
                "surefire.test.class.path", System.getProperty("java.class.path"));
        List<Path> runtimeClassPath = Arrays.stream(testClassPath.split(System.getProperty("path.separator")))
                .map(Path::of)
                .toList();

        URL[] urls = runtimeClassPath.stream()
                .map(Path::toUri)
                .map(uri -> {
                    try {
                        return uri.toURL();
                    } catch (Exception e) {
                        throw new IllegalArgumentException(e);
                    }
                })
                .toArray(URL[]::new);
        try (URLClassLoader classLoader = new URLClassLoader(urls, ClassLoader.getPlatformClassLoader())) {
            Class<?> builderApi = Class.forName("io.oxia.client.api.OxiaClientBuilder", true, classLoader);
            Object builder = builderApi.getMethod("create", String.class).invoke(null, "127.0.0.1:6648");

            assertEquals("io.oxia.client.OxiaClientBuilderImpl", builder.getClass().getName());
        }
    }

    @Test
    void testCreateBuilderRejectsNonStringJsonConfigValue() {
        String configJson = """
                {
                  "authParams": {
                    "token": "abc"
                  }
                }
                """;

        assertThrows(IllegalArgumentException.class, () -> OxiaClientFactory.createBuilder(
                "oxia://127.0.0.1:6648/storage", configJson, OpenTelemetry.noop()));
    }

    @Test
    void testCreateBuilderRejectsInvalidJsonConfig() {
        assertThrows(IllegalArgumentException.class, () -> OxiaClientFactory.createBuilder(
                "oxia://127.0.0.1:6648/storage", "{invalid", OpenTelemetry.noop()));
    }

    private static Object getClientConfig(Object builder) throws Exception {
        Method method = builder.getClass().getMethod("getClientConfig");
        return method.invoke(builder);
    }

    private static Object invoke(Object target, String methodName) throws Exception {
        Method method = target.getClass().getMethod(methodName);
        return method.invoke(target);
    }

}
