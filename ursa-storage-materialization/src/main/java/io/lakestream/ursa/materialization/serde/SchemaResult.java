/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde;

// Cache the converted schema to avoid repeated conversion
public record SchemaResult<T>(org.apache.avro.Schema avroSchema, T tableSchema, Exception exception) {

    public SchemaResult(org.apache.avro.Schema avroSchema, T tableSchema) {
        this(avroSchema, tableSchema, null);
    }

    public SchemaResult(T tableSchema) {
        this(null, tableSchema, null);
    }
}
