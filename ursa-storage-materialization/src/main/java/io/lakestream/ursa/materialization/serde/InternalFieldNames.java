/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde;

/**
 * Reserved field names used to thread message metadata (publish time, offsets, properties,
 * schema version) into materialized records. Mirrors the constants historically defined on
 * {@code io.lakestream.ursa.lakehouse.schema.AbstractSchemaConverter}.
 */
public final class InternalFieldNames {

    public static final String INTERNAL_EVENT_TIME = "__eventTime";
    public static final String INTERNAL_PUBLISH_TIME = "__publishTime";
    public static final String INTERNAL_PROPERTIES = "__properties";
    public static final String INTERNAL_SCHEMA_VERSION = "__schemaVersion";
    public static final String INTERNAL_MESSAGE_OFFSET = "__messageOffset";

    private InternalFieldNames() {
    }
}
