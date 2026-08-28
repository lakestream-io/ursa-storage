/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2;

import io.lakestream.api.Stream;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.api.materialization.TableMode;
import io.lakestream.ursa.compaction.DynamicConfigs;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.compact.FailureMessage;
import io.lakestream.ursa.lakehouse.compact.KafkaEntryProcessFactory;
import io.lakestream.ursa.lakehouse.v2.delta.DeltaExternalDLTTableWriter;
import io.lakestream.ursa.lakehouse.v2.delta.DeltaExternalTableWriter;
import io.lakestream.ursa.lakehouse.v2.iceberg.IcebergExternalDLTTableWriter;
import io.lakestream.ursa.lakehouse.v2.iceberg.IcebergExternalTableWriter;
import io.lakestream.ursa.lakehouse.v2.iceberg.IcebergManagedTableWriter;
import io.lakestream.ursa.materialization.MaterializationException;
import io.lakestream.ursa.materialization.MaterializationRuntime;
import io.lakestream.ursa.materialization.serde.EntryFormat;
import io.lakestream.ursa.materialization.serde.EntrySerdeFactory;
import io.lakestream.ursa.materialization.serde.SchemaService;
import io.lakestream.ursa.metrics.InstrumentProvider;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * Package-private helper that wires a {@link TableCatalog} + {@link TableMaterializationPolicy}
 * into the underlying {@link AbstractLakehouseWriter} appropriate for the requested
 * {@link TableCatalogType}.
 *
 * <p>The existing writer hierarchy is driven by a fully-populated
 * {@link LakehouseConfiguration} backed by a flat {@link Properties} map. To preserve that
 * contract without redesigning {@code LakehouseConfiguration}, this helper projects the new
 * {@link TableCatalog#connection() connection} map back into the legacy
 * {@code <type>.catalog.<name>.<key>} prefix format, layers any
 * {@link TableMaterializationPolicy#connectionOverrides() per-stream overrides} on top, and then
 * builds a {@link LakehouseConfiguration} that the existing writers can consume unchanged.
 *
 * <p>This is an explicit T8 trade-off: T8 lands the adapter; refactoring
 * {@code LakehouseConfiguration} to consume {@code TableCatalog} natively belongs to T9.
 */
final class LakehouseWriterFactory {

    private LakehouseWriterFactory() {
    }

    /**
     * Builds an Iceberg writer. {@link TableMode#MANAGED} maps to
     * {@link IcebergManagedTableWriter}; {@link TableMode#EXTERNAL} (and {@link TableMode#CUSTOM}
     * for now) maps to {@link IcebergExternalTableWriter}.
     */
    static AbstractLakehouseWriter iceberg(TableMaterializationPolicy policy,
                                           TableCatalog catalog,
                                           Stream stream,
                                           MaterializationRuntime runtime) {
        requireNonNullArgs(policy, catalog, stream, runtime);
        if (catalog.type() != TableCatalogType.ICEBERG) {
            throw new MaterializationException(
                    io.lakestream.ursa.exception.ExceptionCode.LAKEHOUSE_CREATE_TABLE_WRITER_ERROR,
                    "Expected ICEBERG catalog type but got " + catalog.type());
        }
        LakehouseConfiguration config =
                buildConfiguration(catalog, policy, "iceberg", runtime.entryFormat(), runtime.taskProperties());
        EntrySerdeFactory serdeFactory = new EntrySerdeFactory((SchemaService) runtime.schemaService());
        InstrumentProvider provider = InstrumentProvider.NOOP;
        String topic = destinationTopic(stream);
        String schemaTopic = schemaTopic(stream, runtime.taskProperties());

        TableMode mode = effectiveMode(policy);
        return switch (mode) {
            case MANAGED -> new IcebergManagedTableWriter(
                    topic, schemaTopic, serdeFactory, config, provider);
            case EXTERNAL, CUSTOM ->
                    new IcebergExternalTableWriter(topic, schemaTopic, serdeFactory, config, provider);
        };
    }

    /**
     * Builds a Delta writer. Both {@link TableMode#MANAGED} and {@link TableMode#EXTERNAL} map to
     * {@link DeltaExternalTableWriter} today — the existing Delta hierarchy has no separate "managed"
     * variant for non-UC Delta. T9 can split this if a managed Delta writer is added.
     */
    static AbstractLakehouseWriter delta(TableMaterializationPolicy policy,
                                         TableCatalog catalog,
                                         Stream stream,
                                         MaterializationRuntime runtime) {
        requireNonNullArgs(policy, catalog, stream, runtime);
        if (catalog.type() != TableCatalogType.DELTA) {
            throw new MaterializationException(
                    io.lakestream.ursa.exception.ExceptionCode.LAKEHOUSE_CREATE_TABLE_WRITER_ERROR,
                    "Expected DELTA catalog type but got " + catalog.type());
        }
        LakehouseConfiguration config =
                buildConfiguration(catalog, policy, "delta", runtime.entryFormat(), runtime.taskProperties());
        EntrySerdeFactory serdeFactory = new EntrySerdeFactory((SchemaService) runtime.schemaService());
        String destinationTopic = destinationTopic(stream);
        return new DeltaExternalTableWriter(
                destinationTopic,
                schemaTopic(stream, runtime.taskProperties()),
                serdeFactory, config, InstrumentProvider.NOOP);
    }

    /** Builds a Delta-on-Unity-Catalog writer ({@link DeltaExternalTableWriter}). */
    static AbstractLakehouseWriter deltaUc(TableMaterializationPolicy policy,
                                           TableCatalog catalog,
                                           Stream stream,
                                           MaterializationRuntime runtime) {
        requireNonNullArgs(policy, catalog, stream, runtime);
        if (catalog.type() != TableCatalogType.DELTA_UC) {
            throw new MaterializationException(
                    io.lakestream.ursa.exception.ExceptionCode.LAKEHOUSE_CREATE_TABLE_WRITER_ERROR,
                    "Expected DELTA_UC catalog type but got " + catalog.type());
        }
        LakehouseConfiguration config =
                buildConfiguration(catalog, policy, "delta", runtime.entryFormat(), runtime.taskProperties());
        EntrySerdeFactory serdeFactory = new EntrySerdeFactory((SchemaService) runtime.schemaService());
        String destinationTopic = destinationTopic(stream);
        return new DeltaExternalTableWriter(
                destinationTopic,
                schemaTopic(stream, runtime.taskProperties()),
                serdeFactory, config, InstrumentProvider.NOOP);
    }

    /**
     * Builds the external dead-letter-table (DLT) writer for a stream, used to capture records that
     * fail serde (bad/incompatible schema, malformed payload) so they are not silently dropped. Only
     * the {@link TableMode#EXTERNAL EXTERNAL} path has a DLT (mirrors
     * {@code LakehouseFactory.getExternalDLTWriter}); MANAGED/CUSTOM return empty. The caller
     * registers a {@code DLTFailureMessageHandler} wrapping this writer on the main external writer.
     */
    static Optional<LakehouseRecordWriter<FailureMessage>> externalDltWriter(TableMaterializationPolicy policy,
                                                                       TableCatalog catalog,
                                                                       Stream stream,
                                                                       String prefix,
                                                                       @Nullable EntryFormat entryFormat,
                                                                       Map<String, String> taskProperties) {
        if (effectiveMode(policy) != TableMode.EXTERNAL) {
            return Optional.empty();
        }
        LakehouseConfiguration config = buildConfiguration(catalog, policy, prefix, entryFormat, taskProperties);
        String topic = destinationTopic(stream);
        InstrumentProvider provider = InstrumentProvider.NOOP;
        return switch (config.getLakehouseType()) {
            case ICEBERG -> Optional.of(new IcebergExternalDLTTableWriter(topic, config, provider));
            case DELTA -> config.isDeltaDltEnabled()
                    ? Optional.of(new DeltaExternalDLTTableWriter(topic, config, provider))
                    : Optional.empty();
            default -> Optional.empty();
        };
    }

    /**
     * Projects {@code catalog.connection()} + {@code policy.connectionOverrides()} back into the
     * legacy {@code <prefix>.catalog.<name>.<key>} key-space, then layers
     * {@code catalog.properties()} as bare top-level keys. The order is:
     * <ol>
     *   <li>{@code catalog.properties()} — catalog-level tuning defaults (lowest priority);</li>
     *   <li>{@code catalog.connection()} as {@code <prefix>.catalog.<name>.<key>=<value>};</li>
     *   <li>{@code policy.connectionOverrides()} as {@code <prefix>.catalog.<name>.<key>=<value>}
     *       (highest priority — per-stream overrides win).</li>
     * </ol>
     * The catalog name is recorded under {@code catalog.name} so
     * {@link LakehouseConfiguration#getCatalogName()} resolves correctly.
     */
    static LakehouseConfiguration buildConfiguration(TableCatalog catalog,
                                                     TableMaterializationPolicy policy,
                                                     String prefix,
                                                     @Nullable EntryFormat entryFormat,
                                                     Map<String, String> taskProperties) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(policy, "policy");
        Objects.requireNonNull(prefix, "prefix");

        Properties properties = new Properties();
        // The source entry format selects the source-aware encoder. When the orchestrator did not
        // resolve one, leave it unset so the writer
        // falls back to its historical default.
        if (entryFormat != null) {
            properties.setProperty("entryFormat", entryFormat.name());
        }
        // 1) Bare catalog properties at the top level.
        for (Map.Entry<String, String> e : catalog.properties().entrySet()) {
            properties.setProperty(e.getKey(), e.getValue());
        }
        // 2) The catalog connection, re-prefixed under <type>.catalog.<name>.<key>.
        String catalogPrefix = prefix + ".catalog." + catalog.name() + ".";
        // catalog.connection() is for ClickHouse
        // catalog.catalogProperties is for Lakehouse
        for (Map.Entry<String, String> e : catalog.connection().entrySet()) {
            properties.setProperty(catalogPrefix + e.getKey(), e.getValue());
        }
        // 3) Stream-level connection overrides win.
        for (Map.Entry<String, String> e : policy.connectionOverrides().entrySet()) {
            properties.setProperty(catalogPrefix + e.getKey(), e.getValue());
        }
        // Make sure the catalog name is wired through so getCatalogName() resolves.
        properties.setProperty(LakehouseConfiguration.CATALOG_NAME, catalog.name());
        // Mirror the lakehouse type onto the legacy enum so writers that branch on it work.
        TableCatalogType type = catalog.type();
        if (type == TableCatalogType.ICEBERG) {
            properties.setProperty("lakehouseType", LakehouseConfiguration.LakehouseType.ICEBERG.name());
        } else if (type == TableCatalogType.DELTA || type == TableCatalogType.DELTA_UC) {
            properties.setProperty("lakehouseType", LakehouseConfiguration.LakehouseType.DELTA.name());
        }
        // Mirror the table mode onto the legacy enum so writers route correctly.
        properties.setProperty("streamTableMode", legacyMode(policy).name());
        // Back-compat: project the task's legacy DynamicConfigs onto the flat keys the writers read,
        // so deployments that drove materialization through task properties behave the same on the
        // policy-based pipeline. Task properties take precedence over the catalog/policy-derived values.
        applyTaskPropertyOverrides(properties, taskProperties);
        return new LakehouseConfiguration(properties);
    }

    /**
     * Projects the task's legacy {@link DynamicConfigs} (carried in the per-task compaction properties)
     * onto the flat {@link LakehouseConfiguration} keys the writers read: {@code catalog.name}
     * ({@code sdtCatalogName}), {@code identifierFields}, {@code partitionKey}, {@code upsertMode}
     * ({@code upsertModeEnabled}), and {@code base.schema.version} ({@code baseSchemaVersion}). Applied
     * last so per-task values win over the catalog/policy-derived configuration.
     */
    private static void applyTaskPropertyOverrides(Properties properties, Map<String, String> taskProperties) {
        if (taskProperties == null || taskProperties.isEmpty()) {
            return;
        }
        String deltaDltEnabled = taskProperties.get(LakehouseConfiguration.DELTA_DLT_ENABLED);
        if (deltaDltEnabled != null) {
            properties.setProperty(LakehouseConfiguration.DELTA_DLT_ENABLED, deltaDltEnabled);
        }
        DynamicConfigs dc = DynamicConfigs.fromTaskProperties(taskProperties);
        dc.sdtCatalogName().filter(s -> !s.isBlank())
                .ifPresent(v -> properties.setProperty(LakehouseConfiguration.CATALOG_NAME, v));
        dc.identifierFields().filter(s -> !s.isBlank())
                .ifPresent(v -> properties.setProperty("identifierFields", v));
        dc.partitionKey().filter(s -> !s.isBlank())
                .ifPresent(v -> properties.setProperty("partitionKey", v));
        dc.upsertModeEnabled()
                .ifPresent(v -> properties.setProperty("upsertMode", String.valueOf(v)));
        dc.baseSchemaVersion()
                .ifPresent(v -> properties.setProperty("base.schema.version", String.valueOf(v)));
    }

    private static LakehouseConfiguration.StreamTableMode legacyMode(TableMaterializationPolicy policy) {
        return switch (effectiveMode(policy)) {
            case MANAGED -> LakehouseConfiguration.StreamTableMode.MANAGED;
            case EXTERNAL -> LakehouseConfiguration.StreamTableMode.EXTERNAL;
            case CUSTOM -> LakehouseConfiguration.StreamTableMode.CUSTOM;
        };
    }

    private static TableMode effectiveMode(TableMaterializationPolicy policy) {
        return policy.table()
                .flatMap(t -> t.mode())
                .orElse(TableMode.MANAGED);
    }

    static String destinationTopic(Stream stream) {
        StreamIdentifier id = stream.identifier();
        return id.fullName();
    }

    static String schemaTopic(Stream stream, Map<String, String> taskProperties) {
        return KafkaEntryProcessFactory.resolveSchemaTopic(destinationTopic(stream), taskProperties);
    }

    private static void requireNonNullArgs(TableMaterializationPolicy policy,
                                           TableCatalog catalog,
                                           Stream stream,
                                           MaterializationRuntime runtime) {
        Objects.requireNonNull(policy, "policy");
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(stream, "stream");
        Objects.requireNonNull(runtime, "runtime");
    }
}
