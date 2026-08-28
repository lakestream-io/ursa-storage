/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/**
 * SchemaCache is a cache for storing the schemas you make a transform based on the original schema.
 * In the encoder or decoder, we need to transform the messages schema to the lakehouse schema, but
 * we can avoid doing it every time. Only if the schema is changed, we need to transform it again.
 *
 * Most of the time, the schema is identity by a unique id, so we can use the id as the key. But you
 * can also define your own key, such as the schema itself. The key must be unique.
 */
public class SchemaCache {

    private static final int MAX_SCHEMA_SIZE = Integer.parseInt(
        System.getProperty("ursa.maxSchemaCacheSize", "10000"));

    private static final Duration MAX_SCHEMA_AGE = Duration.ofMinutes(
        Long.parseLong(System.getProperty("ursa.maxSchemaCacheAge", "10")));

    public static final SchemaCache INSTANCE = new SchemaCache();

    private final Cache<Object, Object> cache;

    @VisibleForTesting
    public SchemaCache(int maxSchemaSize, Duration maxSchemaAge) {
        this.cache = CacheBuilder.newBuilder()
            .expireAfterWrite(maxSchemaAge)
            .maximumSize(maxSchemaSize)
            .build();
    }

    public SchemaCache() {
        this.cache = CacheBuilder.newBuilder()
            .expireAfterWrite(MAX_SCHEMA_AGE)
            .maximumSize(MAX_SCHEMA_SIZE)
            .build();
    }

    public Object computeIfAbsent(Object schemaKey, Supplier<Object> loadSchema)
        throws ExecutionException {
        return cache.get(schemaKey, loadSchema::get);
    }

    public Object get(Object schemaKey) {
        return cache.getIfPresent(schemaKey);
    }

    public void invalidate(Object schemaKey) {
        cache.invalidate(schemaKey);
    }
}
