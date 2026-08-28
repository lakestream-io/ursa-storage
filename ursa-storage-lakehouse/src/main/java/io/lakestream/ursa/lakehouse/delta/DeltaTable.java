/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Transaction;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.transaction.UpdateTableTransactionBuilder;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.catalog.unity.UnityCatalogApi;
import io.lakestream.ursa.lakehouse.catalog.unity.UnityTableIdentifier;
import io.lakestream.ursa.lakehouse.exception.LakehouseException;
import io.lakestream.ursa.lakehouse.iceberg.exception.SchemaEvolutionException;
import io.lakestream.ursa.lakehouse.iceberg.exception.SchemaMappingException;
import io.lakestream.ursa.lakehouse.utils.VersionUtils;
import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public abstract class DeltaTable {

    public static final String URSA_DELTA_ENGINE = "ursa-storage-" + VersionUtils.PROJECT_VERSION;

    public static final String LAKESTREAM_SCHEMA_MAPPING = "lakestream.schema.mapping";
    public static final String SCHEMA_EVOLUTION_SOFT_DELETE_ENABLED = "schema.evolution.soft-delete.enabled";

    static final Set<String> TAG_KEYS = new HashSet<>(Arrays.asList("topic", "streamId", "endOffset"));

    public static final String ORDER_TAG = "order";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    protected final LakehouseConfiguration config;
    @Getter
    protected final String parentTopic;
    protected final List<String> partitionKeys;
    @VisibleForTesting
    @Getter
    protected Engine engine;

    protected UnityCatalogApi unityCatalogApi;

    @Getter
    protected String tableLocation;

    public DeltaTable(LakehouseConfiguration config, String parentTopic) {
        this.config = config;
        this.parentTopic = parentTopic;
        String partitionKey = config.getPartitionKey();
        if (StringUtils.isBlank(partitionKey) || LakehouseConfiguration.NONE_PARTITION_KEY.equals(partitionKey)) {
            partitionKeys = Collections.emptyList();
        } else {
            partitionKeys = Arrays.asList(Arrays.stream(partitionKey.split(","))
                .map(String::strip).toArray(String[]::new));
        }
        this.unityCatalogApi = UnityCatalogApi.getInstance(config);
    }

    protected String getUnityCatalogName() {
        return config.getUnityCatalogName();
    }

    protected UnityTableIdentifier getUnityTableIdentifier() {
        return UnityTableIdentifier.parse(parentTopic);
    }

    abstract void refreshTable();

    public abstract boolean tableExists();

    public abstract void createDeltaTable(Long schemaVersion, StructType deltaSchema);

    public void evolveSchemaWithVersion(long versionId, StructType deltaSchema)
        throws SchemaMappingException, SchemaEvolutionException {
        refreshTable();
        SnapshotImpl latestSnapshot = (SnapshotImpl) getLatestSnapshot();
        var schemaMapping = getSchemaMapping(latestSnapshot);
        if (schemaMapping.contains(versionId)) {
            log.info("Schema version {} already exists, skipping evolution for table: {}", versionId, parentTopic);
            return;
        }
        latestSnapshot = updateSchemaEvolutionTablePropertiesIfNeeded(latestSnapshot);
        AtomicInteger columId;
        String maxIdStr =
            latestSnapshot.getMetadata().getConfiguration().get(ColumnMapping.COLUMN_MAPPING_MAX_COLUMN_ID_KEY);
        if (maxIdStr != null) {
            columId = new AtomicInteger(Integer.parseInt(maxIdStr));
        } else {
            columId = new AtomicInteger(0);
        }

        StructType oldSchema = latestSnapshot.getMetadata().getSchema();
        boolean softDeleteEnabled = isSoftDeleteEnabled(latestSnapshot);

        StructType newSchema = CustomColumnMapping.assignColumnIdAndPhysicalNameForTableEvolution(oldSchema,
        deltaSchema, columId, softDeleteEnabled, config.makeNewFieldsOptionalOnEvolution());

        UpdateTableTransactionBuilder txBuilder =
            latestSnapshot.buildUpdateTableTransaction(URSA_DELTA_ENGINE, Operation.MANUAL_UPDATE)
                .withUpdatedSchema(newSchema);

        Map<String, String> newConfigs = new HashMap<>();
        String schemaMappingStr = latestSnapshot.getMetadata().getConfiguration().get(LAKESTREAM_SCHEMA_MAPPING);
        Set<Long> schemaMappings = parseSchemaMapping(schemaMappingStr);
        schemaMappings.add(versionId);
        newConfigs.put(LAKESTREAM_SCHEMA_MAPPING, schemaMappingToString(schemaMappings));
        txBuilder.withTablePropertiesAdded(newConfigs);
        try {
            Transaction tx = txBuilder.build(engine);
            tx.commit(engine, CloseableIterable.emptyIterable());
        } catch (KernelException e) {
            throw new SchemaEvolutionException(e.getMessage(), e);
        }
    }

    private SnapshotImpl updateSchemaEvolutionTablePropertiesIfNeeded(SnapshotImpl latestSnapshot) {
        Map<String, String> tablePropertiesToUpdate = new HashMap<>();
        Map<String, String> configuration = latestSnapshot.getMetadata().getConfiguration();

        String columnMappingMode = configuration.get(TableConfig.COLUMN_MAPPING_MODE.getKey());
        if (columnMappingMode == null) {
            tablePropertiesToUpdate.put(
                TableConfig.COLUMN_MAPPING_MODE.getKey(),
                ColumnMapping.ColumnMappingMode.NAME.toString());
        }

        String typeWideningEnabled = configuration.get(TableConfig.TYPE_WIDENING_ENABLED.getKey());
        if (typeWideningEnabled == null) {
            tablePropertiesToUpdate.put(TableConfig.TYPE_WIDENING_ENABLED.getKey(), "true");
        }

        if (tablePropertiesToUpdate.isEmpty()) {
            return latestSnapshot;
        }

        Transaction txn =
            latestSnapshot.buildUpdateTableTransaction(URSA_DELTA_ENGINE, Operation.MANUAL_UPDATE)
                .withTablePropertiesAdded(tablePropertiesToUpdate)
                .build(this.engine);
        txn.commit(engine, CloseableIterable.emptyIterable());
        return (SnapshotImpl) getLatestSnapshot();
    }

    protected Map<String, String> buildCreateTableProperties(Long schemaVersion) throws SchemaMappingException {
        Map<String, String> properties = new HashMap<>(config.getDeltaProperties());
        properties.put(TableConfig.COLUMN_MAPPING_MODE.getKey(), ColumnMapping.ColumnMappingMode.NAME.toString());
        if (schemaVersion != null) {
            properties.put(LAKESTREAM_SCHEMA_MAPPING, schemaMappingToString(Collections.singleton(schemaVersion)));
        }
        return properties;
    }

    protected boolean isSoftDeleteEnabled(SnapshotImpl latestSnapshot) {
        String tableProperty =
            latestSnapshot.getMetadata().getConfiguration().get(SCHEMA_EVOLUTION_SOFT_DELETE_ENABLED);
        if (tableProperty != null) {
            return Boolean.parseBoolean(tableProperty);
        }
        return config.getProperties()
            .getProperty(SCHEMA_EVOLUTION_SOFT_DELETE_ENABLED, "true")
            .equalsIgnoreCase("true");
    }

    public String schemaMappingToString(Set<Long> set) throws SchemaMappingException {
        try {
            return OBJECT_MAPPER.writeValueAsString(set);
        } catch (JsonProcessingException e) {
            throw new SchemaMappingException("Failed to save schema mapping for table: " + parentTopic, e);
        }
    }

    public abstract Snapshot getLatestSnapshot();

    public CloseableIterator<AddFileAction> getTableAddActionIterator() throws IOException {
        refreshTable();
        return DeltaTableUtils.getAddActionIterator(getLatestSnapshot(), engine);
    }

    protected Set<Long> parseSchemaMapping(String str) throws SchemaMappingException {
        if (StringUtils.isEmpty(str)) {
            return new HashSet<>();
        }
        try {
            return OBJECT_MAPPER.readValue(str, new TypeReference<Set<Long>>() {
            });
        } catch (JsonProcessingException e) {
            throw new SchemaMappingException("Failed to get schema mapping for table: " + parentTopic, e);
        }
    }

    public long commit(List<ParquetFileStat> fileStats)
        throws LakehouseException {
        refreshTable();
        if (!tableExists()) {
            throw new LakehouseException("Table not exists for topic: " + parentTopic);
        }
        fileStats.sort((stat1, stat2) -> {
            Map<String, String> tags1 = stat1.getTags();
            Map<String, String> tags2 = stat2.getTags();

            if (tags1 == null && tags2 == null) {
                return 0;
            }
            if (tags1 == null) {
                return 1;
            }
            if (tags2 == null) {
                return -1;
            }

            long streamId1 = Long.parseLong(tags1.getOrDefault("streamId", "-1"));
            long streamId2 = Long.parseLong(tags2.getOrDefault("streamId", "-1"));

            long endOffset1 = Long.parseLong(tags1.getOrDefault("endOffset", "-1"));
            long endOffset2 = Long.parseLong(tags2.getOrDefault("endOffset", "-1"));

            int streamIdCompare = Long.compare(streamId2, streamId1);

            if (streamIdCompare != 0) {
                return streamIdCompare;
            }
            return Long.compare(endOffset2, endOffset1);
        });
        List<Row> filesToCommit = buildAddFileAction(fileStats);
        commitSnapshot(filesToCommit);
        // TODO: return the latest metadata file size
        return -1L;
    }

    public void delete(List<ParquetFileStat> fileStats) throws LakehouseException {
        refreshTable();
        if (!tableExists()) {
            throw new LakehouseException("Table not exists for topic: " + parentTopic);
        }
        log.info("Delete delta table files for topic: {}, fileStats: {}", parentTopic, fileStats);
        var rows = new ArrayList<Row>();
        for (ParquetFileStat fileStat : fileStats) {
            var row = DeltaTableUtils.buildRemoveFileAction(
                fileStat.getFilePath(), fileStat.getFileSize(),
                System.currentTimeMillis(), fileStat.getPartitionValues(), true, fileStat.getStats(),
                fileStat.getTags());
            rows.add(row);
        }
        commitSnapshot(rows);
    }

    public Set<Long> getSchemaMapping() throws SchemaMappingException {
        refreshTable();
        Snapshot latestSnapshot = getLatestSnapshot();
        if (latestSnapshot == null) {
            return Collections.emptySet();
        }
        return getSchemaMapping((SnapshotImpl) latestSnapshot);
    }

    protected Set<Long> getSchemaMapping(SnapshotImpl snapshot) throws SchemaMappingException {
        Map<String, String> configuration = snapshot.getMetadata().getConfiguration();
        String schemaTagStr = configuration.get(LAKESTREAM_SCHEMA_MAPPING);
        return parseSchemaMapping(schemaTagStr);
    }

    public abstract List<Row> buildAddFileAction(List<ParquetFileStat> fileStats);

    public abstract void commitSnapshot(List<Row> actions);
}
