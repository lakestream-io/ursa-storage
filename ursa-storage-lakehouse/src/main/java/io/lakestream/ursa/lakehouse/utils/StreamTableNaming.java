/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils;

import io.lakestream.api.SourceMetadataProperties;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.api.materialization.TableMode;
import io.lakestream.api.materialization.TableNaming;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/** Resolves writer destinations and carries their final identity to asynchronous committers. */
public final class StreamTableNaming {

    /** Overrides the table name a stream materializes into. See {@code TableNaming} for the syntax. */
    public static final String TABLE_NAME_TEMPLATE_PROPERTY = "tableNameTemplate";

    /** Internal task property containing the final resolved table namespace. */
    public static final String RESOLVED_TABLE_NAMESPACE_PROPERTY =
            "lakestream.materialization.resolved.table.namespace";

    /** Internal task property containing the final resolved table name. */
    public static final String RESOLVED_TABLE_NAME_PROPERTY =
            "lakestream.materialization.resolved.table.name";

    private StreamTableNaming() {
    }

    /** Stores the final table identity in writer configuration. */
    public static void applyResolvedTableIdentifier(Properties properties, TableIdentifier identifier) {
        Objects.requireNonNull(properties, "properties");
        Objects.requireNonNull(identifier, "identifier");
        properties.setProperty(RESOLVED_TABLE_NAMESPACE_PROPERTY, identifier.namespace());
        properties.setProperty(RESOLVED_TABLE_NAME_PROPERTY, identifier.name());
    }

    /** Returns an immutable task-property snapshot carrying the final table identity. */
    public static Map<String, String> withResolvedTableIdentifier(
            Map<String, String> properties, TableIdentifier identifier) {
        Objects.requireNonNull(identifier, "identifier");
        Map<String, String> resolved = new HashMap<>(properties == null ? Map.of() : properties);
        resolved.put(RESOLVED_TABLE_NAMESPACE_PROPERTY, identifier.namespace());
        resolved.put(RESOLVED_TABLE_NAME_PROPERTY, identifier.name());
        return Map.copyOf(resolved);
    }

    /** Returns {@code namespace/name}, the legacy writer representation of a table identifier. */
    public static String qualifiedName(TableIdentifier identifier) {
        Objects.requireNonNull(identifier, "identifier");
        return identifier.namespace() + "/" + identifier.name();
    }

    /** Returns the dead-letter table next to {@code identifier}. */
    public static TableIdentifier deadLetterTable(TableIdentifier identifier, String suffix) {
        Objects.requireNonNull(identifier, "identifier");
        Objects.requireNonNull(suffix, "suffix");
        return new TableIdentifier(identifier.namespace(), identifier.name() + suffix);
    }

    /**
     * Resolves the table for {@code logName} on a commit or cleanup path.
     *
     * <p>New tasks carry the exact identifier chosen by {@code ResolvedMaterialization}; that value
     * always wins. Tasks created by older versions fall back to their naming template, then to the
     * historical stream-name behaviour. Keeping the last fallback unchanged lets an old compacted
     * task commit to the same table its old writer created.
     */
    public static TableIdentifier resolve(String logName, Properties properties) {
        return resolve(logName, properties, false);
    }

    /**
     * Resolves the destination a writer should create when no final identifier has been persisted yet.
     * EXTERNAL/CUSTOM writers use source logical-name metadata by default; MANAGED writers keep the
     * storage stream identity. Once the writer completes, its result must persist this identifier so
     * the asynchronous committer can use {@link #resolve(String, Properties)} exactly.
     */
    public static TableIdentifier resolveForWriter(String logName, Properties properties) {
        String mode = properties == null ? null : properties.getProperty("streamTableMode");
        boolean logicalNameDefault = "EXTERNAL".equalsIgnoreCase(mode) || "CUSTOM".equalsIgnoreCase(mode);
        return resolve(logName, properties, logicalNameDefault);
    }

    /**
     * Resolves a writer destination when the caller already knows the effective table mode. This is
     * used by the legacy worker after it has selected an external writer, even when an old task does
     * not carry {@code streamTableMode}.
     */
    public static TableIdentifier resolveForWriter(
            String logName, Properties properties, TableMode mode) {
        Objects.requireNonNull(mode, "mode");
        return resolve(logName, properties,
                mode == TableMode.EXTERNAL || mode == TableMode.CUSTOM);
    }

    private static TableIdentifier resolve(
            String logName, Properties properties, boolean logicalNameDefault) {
        Optional<TableIdentifier> resolved = resolvedTableIdentifier(properties);
        if (resolved.isPresent()) {
            return resolved.get();
        }

        TopicName identity = TopicName.getStreamIdentity(logName);
        StreamIdentifier stream = StreamIdentifier.of(identity.getNamespace(), identity.getLocalName());
        String template = properties == null ? null : properties.getProperty(TABLE_NAME_TEMPLATE_PROPERTY);
        if (template != null) {
            return new TableNaming(Optional.empty(), template).toTableIdentifier(stream, asMap(properties));
        }
        String tableName = logicalNameDefault
                ? SourceMetadataProperties.logicalName(stream, asMap(properties))
                : stream.name();
        return new TableIdentifier(stream.namespace(), tableName);
    }

    private static Optional<TableIdentifier> resolvedTableIdentifier(Properties properties) {
        if (properties == null) {
            return Optional.empty();
        }
        String namespace = properties.getProperty(RESOLVED_TABLE_NAMESPACE_PROPERTY);
        String name = properties.getProperty(RESOLVED_TABLE_NAME_PROPERTY);
        if (namespace == null && name == null) {
            return Optional.empty();
        }
        if (namespace == null || namespace.isBlank() || name == null || name.isBlank()) {
            throw new IllegalArgumentException("Incomplete resolved table identifier in task properties");
        }
        return Optional.of(new TableIdentifier(namespace, name));
    }

    private static Map<String, String> asMap(Properties properties) {
        if (properties == null) {
            return Map.of();
        }
        Map<String, String> map = new HashMap<>();
        for (String key : properties.stringPropertyNames()) {
            map.put(key, properties.getProperty(key));
        }
        return map;
    }
}
