/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.clickhouse;

import io.lakestream.api.StreamMetadata;
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
import io.lakestream.ursa.materialization.serde.EntrySerdeFactory;
import io.lakestream.ursa.materialization.serde.EntrySerdeFactory.SerdeType;
import io.lakestream.ursa.materialization.serde.SchemaService;
import io.lakestream.ursa.materialization.serde.TableSchemaService;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSchemaService;
import io.lakestream.ursa.materialization.serde.kafka.KafkaSourceMetadata;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    @Override
    public TableCatalogType catalogType() {
        return TableCatalogType.CLICKHOUSE;
    }

    @Override
    public TableMaterializer<?> create(TableMaterializationPolicy policy,
                                       TableCatalog resolvedCatalog,
                                       StreamMetadata streamMetadata,
                                       MaterializationRuntime runtime) {
        Objects.requireNonNull(policy, "policy");
        Objects.requireNonNull(resolvedCatalog, "resolvedCatalog");
        Objects.requireNonNull(streamMetadata, "streamMetadata");
        Objects.requireNonNull(runtime, "runtime");

        if (resolvedCatalog.type() != TableCatalogType.CLICKHOUSE) {
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                    "ClickHouseTableMaterializerFactory invoked with non-CLICKHOUSE catalog "
                            + resolvedCatalog.name() + " (type=" + resolvedCatalog.type() + ")");
        }

        TableIdentifier tableId = resolveTableIdentifier(policy, streamMetadata);
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
            EntryEncoder<Map<String, Object>> rowEncoder = null;
            if (sourceSchemaService instanceof KafkaSchemaService) {
                rowEncoder = new EntrySerdeFactory(sourceSchemaService).getEncoder(SerdeType.KAFKA_CLICKHOUSE);
            }
            if (rowEncoder != null) {
                String sourceTopic = sourceTopic(streamMetadata, runtime);
                // Share the materializer's JDBC connection so the schema-aware path can create/evolve the
                // destination table (derived from the decoded row shape) before the first INSERT.
                ClickHouseTableSchemaService chSchemaService = new ClickHouseTableSchemaService(
                        connection, tableId, engine, primaryKey, sourceTopic);
                return new ClickHouseTableMaterializer(connection, tableId, engine, primaryKey, batchSize,
                        chSchemaService, rowEncoder, sourceTopic);
            }
            return new ClickHouseTableMaterializer(
                    connection, tableId, engine, primaryKey, batchSize,
                    null, null, null);
        } catch (RuntimeException | Error constructionFailure) {
            try {
                connection.close();
            } catch (Exception | Error closeFailure) {
                constructionFailure.addSuppressed(closeFailure);
            }
            throw constructionFailure;
        }
    }

    static String sourceTopic(StreamMetadata streamMetadata, MaterializationRuntime runtime) {
        return KafkaSourceMetadata.topicName(
                streamMetadata.identifier().fullName(), runtime.taskProperties());
    }

    @Override
    @Nullable
    public TableSchemaService<?, ?> schemaService(TableMaterializationPolicy policy,
                                                  TableCatalog resolvedCatalog,
                                                  StreamMetadata streamMetadata) {
        Objects.requireNonNull(policy, "policy");
        Objects.requireNonNull(resolvedCatalog, "resolvedCatalog");
        Objects.requireNonNull(streamMetadata, "streamMetadata");

        if (resolvedCatalog.type() != TableCatalogType.CLICKHOUSE) {
            throw new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                    "ClickHouseTableMaterializerFactory invoked with non-CLICKHOUSE catalog "
                            + resolvedCatalog.name() + " (type=" + resolvedCatalog.type() + ")");
        }

        TableIdentifier tableId = resolveTableIdentifier(policy, streamMetadata);
        ClickHouseTableEngine engine = ClickHouseTableEngine.forPolicy(policy);
        List<String> primaryKey = policy.primaryKey().orElseGet(List::of);
        String streamId = streamMetadata.identifier().fullName();

        Connection connection = ClickHouseConnectionFactory.open(resolvedCatalog, policy);
        return new ClickHouseTableSchemaService(connection, tableId, engine, primaryKey, streamId);
    }

    /**
     * Resolves the destination table identifier. Prefers the policy's explicit
     * {@link TableMaterializationPolicy#tableIdentifier()} (used by the
     * orchestrator after {@link io.lakestream.api.materialization.ResolvedMaterialization}
     * resolution). The factory never performs catalog resolution itself.
     */
    private static TableIdentifier resolveTableIdentifier(TableMaterializationPolicy policy,
                                                          StreamMetadata streamMetadata) {
        return policy.tableIdentifier()
                .map(ClickHouseTableMaterializerFactory::sanitize)
                .orElseThrow(() -> new MaterializationException(ExceptionCode.INTERNAL_ERROR,
                        "Resolved ClickHouse policy for stream "
                                + streamMetadata.identifier().fullName()
                                + " has no table identifier"));
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
