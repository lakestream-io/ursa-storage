/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

public final class OxiaClientFactory {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, String>> CONFIG_TYPE = new TypeReference<>() {};
    private static final String OXIA_PREFIX = "oxia://";
    private static final String DEFAULT_NAMESPACE = "default";

    private OxiaClientFactory() {
    }

    public static AsyncOxiaClient create(
            String metadataURL,
            @Nullable String configJson,
            OpenTelemetry openTelemetry) throws Exception {
        return createBuilder(metadataURL, configJson, openTelemetry).asyncClient().get();
    }

    static OxiaClientBuilder createBuilder(
            String metadataURL,
            @Nullable String configJson,
            OpenTelemetry openTelemetry) {
        Pair<String, String> oxiaUrl = validateOxiaUrl(metadataURL);
        OxiaClientBuilder builder = OxiaClientBuilder.create(oxiaUrl.getLeft())
                .openTelemetry(openTelemetry)
                .namespace(oxiaUrl.getRight());
        Properties properties = loadConfig(configJson);
        if (!properties.isEmpty()) {
            builder.loadConfig(properties);
        }
        return builder;
    }

    private static Properties loadConfig(@Nullable String configJson) {
        Properties properties = new Properties();
        if (StringUtils.isBlank(configJson)) {
            return properties;
        }
        try {
            Map<String, String> values = OBJECT_MAPPER.readValue(configJson, CONFIG_TYPE);
            for (Map.Entry<String, String> entry : values.entrySet()) {
                if (entry.getValue() != null) {
                    properties.setProperty(entry.getKey(), entry.getValue());
                }
            }
            return properties;
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid Oxia client config JSON.", e);
        }
    }

    public static Pair<String, String> validateOxiaUrl(String metadataURL) {
        if (StringUtils.isBlank(metadataURL)) {
            throw new IllegalArgumentException("Invalid metadata URL. Must start with 'oxia://'.");
        }
        String addressWithNamespace = metadataURL.startsWith(OXIA_PREFIX)
                ? metadataURL.substring(OXIA_PREFIX.length())
                : metadataURL;
        String[] split = addressWithNamespace.split("/");
        if (split.length != 2 && split.length != 1) {
            throw new IllegalArgumentException("Invalid metadata URL. The oxia metadata format should be "
                    + "'oxia://host:port/[namespace]'.");
        }
        return Pair.of(split[0], (split.length > 1) ? split[1] : DEFAULT_NAMESPACE);
    }
}
