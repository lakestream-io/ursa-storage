/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse;

import io.lakestream.api.Stream;
import io.lakestream.api.materialization.ResolvedMaterialization;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableIdentifier;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.ursa.clickhouse.serde.ClickHouseSerdeRegistry;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.materialization.MaterializationException;
import io.lakestream.ursa.materialization.MaterializationRuntime;
import io.lakestream.ursa.materialization.TableMaterializer;
import io.lakestream.ursa.materialization.TableMaterializerFactory;
import io.lakestream.ursa.materialization.serde.EntryEncoder;
import io.lakestream.ursa.materialization.serde.EntryFormat;
import io.lakestream.ursa.materialization.serde.EntrySerdeFactory;
import io.lakestream.ursa.materialization.serde.EntrySerdeFactory.SerdeType;
import io.lakestream.ursa.materialization.serde.SchemaService;
import io.lakestream.ursa.materialization.serde.TableSchemaService;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSchemaService;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * {@link TableMaterializerFactory} for
 * {@link TableCatalogType#CLICKHOUSE CLICKHOUSE} catalogs.
 *
 * <p>Registered via {@code META-INF/services/io.lakestream.ursa.materialization.TableMaterializerFactory}
 * so {@code LakehouseMaterializationService}'s {@link java.util.ServiceLoader}-based dispatch
 * (T9/T10) picks it up automatically when a stream's effective materialization resolves to a
 * ClickHouse catalog.
 */
public final class ClickHouseTableMaterializerFactory implements TableMaterializerFactory {

    private static final Pattern PARTITION_SUFFIX = Pattern.compile("-partition-\\d+$");

    @Override
    public TableCatalogType catalogType() {
        return TableCatalogType.CLICKHOUSE;
    }

    @Override
    public TableMaterializer<?> create(TableMaterializationPolicy policy,
                                       TableCatalog resolvedCatalog,
                                       Stream stream,
                                       MaterializationRuntime runtime) {
        Objects.requireNonNull(policy, "policy");
        Objects.requireNonNull(resolvedCatalog, "resolvedCatalog");
        Objects.requireNonNull(stream, "stream");
        Objects.requireNonNull(runtime, "runtime");

        if (resolvedCatalog.type() != TableCatalogType.CLICKHOUSE) {
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                    "ClickHouseTableMaterializerFactory invoked with non-CLICKHOUSE catalog "
                            + resolvedCatalog.name() + " (type=" + resolvedCatalog.type() + ")");
        }

        TableIdentifier tableId = resolveTableIdentifier(policy, stream);
        ClickHouseTableEngine engine = ClickHouseTableEngine.forPolicy(policy);
        List<String> primaryKey = policy.primaryKey().orElseGet(List::of);
        int batchSize = policy.framework()
                .flatMap(io.lakestream.api.materialization.FrameworkConf::commit)
                .flatMap(io.lakestream.api.materialization.CommitConfig::batchSize)
                .orElse(ClickHouseTableMaterializer.DEFAULT_BATCH_SIZE);

        Connection connection = ClickHouseConnectionFactory.open(resolvedCatalog, policy);
        try {
            // Wire the schema-aware source decoder when the runtime supplies a source schema service.
            // KafkaSchemaService enables schema-aware decoding. When no source schema service is
            // available the materializer falls back to JSON decoding.
            // Ensure the ClickHouse serde providers are registered before constructing the factory.
            ClickHouseSerdeRegistry.ensureRegistered();
            SchemaService<?> sourceSchemaService = runtime.schemaService();
            EntryFormat entryFormat = runtime.entryFormat();
            EntryEncoder<Map<String, Object>> rowEncoder = null;
            if (sourceSchemaService instanceof KafkaSchemaService) {
                rowEncoder = new EntrySerdeFactory(sourceSchemaService).getEncoder(SerdeType.KAFKA_CLICKHOUSE);
            }
            if (rowEncoder != null) {
                String sourceTopic = sourceTopic(stream, runtime);
                // Share the materializer's JDBC connection so the schema-aware path can create/evolve the
                // destination table (derived from the decoded row shape) before the first INSERT.
                ClickHouseTableSchemaService chSchemaService = new ClickHouseTableSchemaService(
                        connection, tableId, engine, primaryKey, sourceTopic);
                return new ClickHouseTableMaterializer(connection, tableId, engine, primaryKey, batchSize,
                        chSchemaService, rowEncoder, entryFormat, sourceTopic);
            }
            return new ClickHouseTableMaterializer(
                    connection, tableId, engine, primaryKey, batchSize,
                    null, null, entryFormat, null);
        } catch (RuntimeException | Error constructionFailure) {
            try {
                connection.close();
            } catch (Exception | Error closeFailure) {
                constructionFailure.addSuppressed(closeFailure);
            }
            throw constructionFailure;
        }
    }

    static String sourceTopic(Stream stream, MaterializationRuntime runtime) {
        String destinationIdentity = stream.identifier().fullName();
        String schemaTopic = runtime.taskProperties().get("sourceSchemaTopic");
        if (schemaTopic != null && !schemaTopic.isBlank()) {
            return schemaTopic;
        }
        String sourceTopic = runtime.taskProperties().get("sourceTopic");
        String legacyTopic = sourceTopic == null || sourceTopic.isBlank()
                ? destinationIdentity : sourceTopic;
        int localNameStart = legacyTopic.lastIndexOf('/') + 1;
        return PARTITION_SUFFIX.matcher(legacyTopic.substring(localNameStart)).replaceFirst("");
    }

    @Override
    @Nullable
    public TableSchemaService<?, ?> schemaService(TableMaterializationPolicy policy,
                                                  TableCatalog resolvedCatalog,
                                                  Stream stream) {
        Objects.requireNonNull(policy, "policy");
        Objects.requireNonNull(resolvedCatalog, "resolvedCatalog");
        Objects.requireNonNull(stream, "stream");

        if (resolvedCatalog.type() != TableCatalogType.CLICKHOUSE) {
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                    "ClickHouseTableMaterializerFactory invoked with non-CLICKHOUSE catalog "
                            + resolvedCatalog.name() + " (type=" + resolvedCatalog.type() + ")");
        }

        TableIdentifier tableId = resolveTableIdentifier(policy, stream);
        ClickHouseTableEngine engine = ClickHouseTableEngine.forPolicy(policy);
        List<String> primaryKey = policy.primaryKey().orElseGet(List::of);
        String streamId = stream.identifier().fullName();

        Connection connection = ClickHouseConnectionFactory.open(resolvedCatalog, policy);
        return new ClickHouseTableSchemaService(connection, tableId, engine, primaryKey, streamId);
    }

    /**
     * Resolves the destination table identifier. Prefers the policy's explicit
     * {@link TableMaterializationPolicy#tableIdentifier()} (used by the
     * orchestrator after {@link io.lakestream.api.materialization.ResolvedMaterialization}
     * resolution) and falls back to the stream's
     * {@link Stream#effectiveMaterialization() effective materialization} when the policy was
     * built without one.
     */
    private static TableIdentifier resolveTableIdentifier(TableMaterializationPolicy policy,
                                                          Stream stream) {
        Optional<TableIdentifier> explicit = policy.tableIdentifier();
        if (explicit.isPresent()) {
            return sanitize(explicit.get());
        }
        Optional<ResolvedMaterialization> resolved = stream.effectiveMaterialization();
        if (resolved.isPresent()) {
            return sanitize(resolved.get().tableIdentifier());
        }
        throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                "ClickHouse policy for stream " + stream.identifier().fullName()
                        + " has no resolvable table identifier");
    }

    /**
     * Folds path components encoded in the table name into a ClickHouse-legal dotted identifier
     * (path separator {@code '/'} → {@code '.'}). The database (namespace component) is
     * the operator-configured ClickHouse database and is left as-is. Applied at both the write and
     * schema-service paths so CREATE TABLE, ALTER, INSERT, and the commit metadata all agree.
     */
    private static TableIdentifier sanitize(TableIdentifier id) {
        return new TableIdentifier(id.namespace(), ClickHouseIdentifiers.sanitizeName(id.name()));
    }
}
