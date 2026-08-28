/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.api;

import java.util.Map;

/**
 * Schema configuration for a stream.
 *
 * @param schemaType the schema type (e.g., "AVRO", "JSON", "PROTOBUF", "NONE")
 * @param properties schema-specific properties
 */
public record SchemaConfig(String schemaType, Map<String, String> properties) {

    /**
     * Creates a schema config with no schema enforcement.
     */
    public SchemaConfig() {
        this("NONE", Map.of());
    }
}
