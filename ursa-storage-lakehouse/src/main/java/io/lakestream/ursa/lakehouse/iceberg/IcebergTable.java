/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig.TABLE_PROP_PREFIX;
import static io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig.WRITE_PROP_PREFIX;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.iceberg.TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;

import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.lakestream.ursa.lakehouse.IcebergCommitter;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.exception.IcebergTableCorruptedException;
import io.lakestream.ursa.lakehouse.exception.LakehouseException;
import io.lakestream.ursa.lakehouse.exception.PersistTagFailedException;
import io.lakestream.ursa.lakehouse.iceberg.exception.SchemaEvolutionException;
import io.lakestream.ursa.lakehouse.iceberg.exception.SchemaMappingException;
import io.lakestream.ursa.lakehouse.utils.TableNameFormatUtils;
import io.lakestream.ursa.lakehouse.utils.TopicName;
import io.lakestream.ursa.lakehouse.v2.MessageId;
import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.types.TypeUtil;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class IcebergTable {
    public static final String ICEBERG_CATALOG_TYPE_S3TABLE = "S3TABLE";
    public static final String ICEBERG_CATALOG_TYPE_BIGQUERY = "BIGQUERY";
    public static final String ICEBERG_CATALOG_TYPE_BIGLAKE = "BIGLAKE";
    public static final String ICEBERG_CATALOG_TYPE_UNITYCATALOG = "UNITYCATALOG";
    public static final String ICEBERG_CATALOG_TYPE_HORIZON = "HORIZON";
    public static final String URSA_KEYS_PROPERTY = "ursa.keys";
    private static final String LAKESTREAM_SCHEMA_MAPPING = "lakestream.schema.mapping";
    private static final Set<String> LAKESTREAM_MANAGED_KEYS = Set.of(LAKESTREAM_SCHEMA_MAPPING);
    // used for compatibility: https://github.com/lakestream-io/ursa-storage/issues/1377
    public static final Set<String> PRESERVED_PROPERTIES =
            Set.of(METADATA_DELETE_AFTER_COMMIT_ENABLED, PARQUET_COMPRESSION, LAKESTREAM_SCHEMA_MAPPING);

    // New property for controlling soft delete behavior
    public static final String SCHEMA_EVOLUTION_SOFT_DELETE_ENABLED = "schema.evolution.soft-delete.enabled";

    private final ReferencedCatalog referencedCatalog;
    @Getter
    private final Catalog catalog;
    private final Optional<String> catalogName;
    @Getter
    private TableIdentifier identifier;
    @Getter
    private TableOptions tableOptions;

    private final LakehouseConfiguration configuration;
    private final int snapshotExpireIntervalInSeconds;
    private long lastSnapshotTriggeredTime = -1;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    @Getter
    @VisibleForTesting
    private Table table;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public IcebergTable(LakehouseConfiguration configuration, TableOptions tableOptions, TableIdentifier identifier) {
        // CatalogFactory.getCatalog already returns a retained reference.
        this(CatalogFactory.getCatalog(configuration), identifier, tableOptions, configuration);
    }

    public IcebergTable(Catalog catalog, TableIdentifier identifier, TableOptions tableOptions,
                        LakehouseConfiguration configuration) {
        this(newRetainedReferencedCatalog(catalog, configuration), identifier, tableOptions, configuration);
    }

    private static ReferencedCatalog newRetainedReferencedCatalog(Catalog catalog,
                                                                  LakehouseConfiguration configuration) {
        ReferencedCatalog rc = new ReferencedCatalog(catalog, configuration.getCatalogMaxOpenTime());
        rc.retain();
        return rc;
    }

    /**
     * Primary constructor. The caller MUST hand over a ReferencedCatalog that has already been
     * retained on their behalf — this constructor does not call retain(). Both entry-point
     * constructors above honor this contract.
     */
    public IcebergTable(ReferencedCatalog referencedCatalog, TableIdentifier identifier, TableOptions tableOptions,
                        LakehouseConfiguration configuration) {
        this.referencedCatalog = referencedCatalog;
        this.catalog = referencedCatalog.getCatalog();
        this.tableOptions = tableOptions;
        this.configuration = configuration;
        this.catalogName = configuration.getCatalogName();
        this.snapshotExpireIntervalInSeconds = configuration.getIcebergSnapshotExpirationInterval();
        this.identifier = TableNameFormatUtils.formatIdentifier(configuration, catalogName, identifier);
        log.info("Initialize IcebergTable with catalog: {}, identifier: {}, tableOptions: {}",
            catalogName, identifier, tableOptions);
    }

    // this constructor need to call create(TableOptions tableOptions, TableIdentifier tableIdentifier) explicitly
    // to initialize the table
    public IcebergTable(LakehouseConfiguration configuration, TableIdentifier identifier) {
        this(configuration, null, identifier);
    }

    public void loadTable() {
        if (table == null) {
            this.table = catalog.loadTable(identifier);
        }
        if (this.table == null) {
            throw new IllegalStateException("Table still being null after load: " + identifier);
        }
    }

    public void create(TableOptions tableOptions) {
        this.tableOptions = tableOptions;
        createIfAbsent();
    }

    public boolean exists() {
        return catalog.tableExists(identifier);
    }

    public void updateTablePartitionSpecIfNeed() {
        loadTable();
        table.refresh();

        // If the tableOptions partition spec is null or empty, and the table spec is also null or empty,
        // skip the update
        if ((tableOptions.getPartitionSpec() == null
            || tableOptions.getPartitionSpec().getPartitionSpec().fields().isEmpty())
            && (table.spec() == null || table.spec().fields().isEmpty())) {
            return;
        }

        if (table.spec() != null
            && Arrays.equals(table.spec().fields().stream().map(PartitionField::name).toArray(),
            tableOptions.getPartitionSpec().getPartitionSpec().fields().stream().map(PartitionField::name).toArray())) {
            return;
        }

        log.info("Update table partition spec from {} to {}", table.spec().toString(), tableOptions.getPartitionSpec());
        UpdatePartitionSpec updatePartitionSpec = table.updateSpec();
        table.spec().fields().forEach(t -> updatePartitionSpec.removeField(t.name()));
        tableOptions.getPartitionSpec().getExpressions().forEach(t -> {
            if (t.targetName() == null) {
                updatePartitionSpec.addField(t.term());
            } else {
                updatePartitionSpec.addField(t.targetName(), t.term());
            }
        });

        updatePartitionSpec.commit();
    }

    public void createIfAbsent() {
        if (table != null) {
            return;
        }
        if (catalog.tableExists(identifier)) {
            this.table = catalog.loadTable(identifier);
            return;
        }

        createNamespace();

        Catalog.TableBuilder tableBuilder = catalog.buildTable(identifier, this.tableOptions.getSchema());
        if (tableOptions.getPartitionSpec() != null) {
            tableBuilder.withPartitionSpec(tableOptions.getPartitionSpec().getPartitionSpec());
        }
        if (tableOptions.getLocation() != null) {
            tableBuilder.withLocation(tableOptions.getLocation());
        }

        Map<String, String> tableProps = tableOptions.getProperties();

        if (IcebergTableUtils.containsV3Types(tableOptions.getSchema())
                && IcebergTableUtils.isV2TableFormat(tableOptions.getProperties())
                && configuration.isAllowIcebergV3()) {
            log.warn("Table properties indicate v2 table format, "
                    + "but schema contains v3 types. Overriding to use v3 table format.");
            tableProps = new HashMap<>(tableOptions.getProperties());
            tableProps.put(FORMAT_VERSION, "3");
        }

        if (tableProps != null && !tableProps.isEmpty()) {
            tableBuilder.withProperties(generateIcebergTableProperties(tableProps));
        }
        log.info("Create table with schema {}", tableOptions.getSchema());
        try {
            this.table = tableBuilder.create();
            log.info("Table created successfully with schema: {}", table.schema());
            saveNameMapping(table, configuration, catalogName);
        } catch (AlreadyExistsException e) {
            log.info("Table already exists: {}", identifier);
            this.table = catalog.loadTable(identifier);
        }
    }

    public void dropTable() {
        catalog.dropTable(identifier);
    }

    private void createNamespace() {
        if (catalog instanceof SupportsNamespaces nsCatalog) {
            if (identifier.hasNamespace()) {
                switch (configuration.getIcebergCatalogBackendType(catalogName)) {
                    case TABULAR -> adaptTabularCreateNamespace(nsCatalog);
                    case NESSIE -> adaptNessieCreateNamespace(nsCatalog);
                    case POLARIS -> adaptPolarisCreateNamespace(nsCatalog);
                    default -> {
                        if (!nsCatalog.namespaceExists(identifier.namespace())) {
                            try {
                                nsCatalog.createNamespace(identifier.namespace());
                            } catch (AlreadyExistsException e) {
                                log.info("Namespace already exists: {}", identifier.namespace());
                            }
                        }
                    }
                }
            }
        }
    }

    private void adaptTabularCreateNamespace(SupportsNamespaces nsCatalog) {
        identifier = TableIdentifier.of(identifier.namespace().toString(), identifier.name());
        for (Namespace namespace : nsCatalog.listNamespaces()) {
            if (namespace.toString().equals(identifier.namespace().toString())) {
                return;
            }
        }
        nsCatalog.createNamespace(identifier.namespace());
    }

    private void adaptNessieCreateNamespace(SupportsNamespaces nsCatalog) {
        Namespace namespace = identifier.namespace();
        for (int i = 0; i < namespace.levels().length; i++) {
            Namespace ns = Namespace.of(Arrays.copyOf(namespace.levels(), i + 1));
            if (!nsCatalog.namespaceExists(ns)) {
                nsCatalog.createNamespace(ns);
            }
        }
    }

    private void adaptPolarisCreateNamespace(SupportsNamespaces nsCatalog) {
        Namespace namespace = identifier.namespace();
        for (int i = 0; i < namespace.levels().length; i++) {
            Namespace ns = Namespace.of(Arrays.copyOf(namespace.levels(), i + 1));
            if (!nsCatalog.namespaceExists(ns)) {
                nsCatalog.createNamespace(ns);
            }
        }
    }

    public long commit(IcebergCommitter.IcebergCommitType lakehouseWriterType,
                       List<ParquetFileStat> fileStats) throws LakehouseException {
        try {
            switch (lakehouseWriterType) {
                case MANAGED -> commitManaged(fileStats);
                case EXTERNAL_ICEBERG -> commitExternal(fileStats);
                default ->
                        throw new IllegalArgumentException("Unsupported lakehouse writer type: " + lakehouseWriterType);
            }
        } catch (Throwable e) {
            if (isTableCorruptedException(e)) {
                throw new IcebergTableCorruptedException("Iceberg table seems corrupted: " + identifier, e);
            }
            throw e;
        }

        if (configuration.getIcebergSnapshotExpirationInterval() > 0) {
            long currentTime = System.currentTimeMillis() / 1000;
            if (lastSnapshotTriggeredTime == -1
                || currentTime - lastSnapshotTriggeredTime > snapshotExpireIntervalInSeconds) {
                table.expireSnapshots().cleanExpiredMetadata(true).commit();
                lastSnapshotTriggeredTime = currentTime;
            }
        }

        long metadataFileSize = getLatestMetadataSize(table);
        log.info("Committed iceberg table: {}, current metadata file size: {}", identifier, metadataFileSize);
        return metadataFileSize;
    }

    private boolean isTableCorruptedException(Throwable throwable) {
        if (throwable instanceof BlobStorageException bse) {
            return bse.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND);
        }
        // extend with aws error in the future
        return false;
    }

    public void updateTableProperties(Map<String, String> properties) {
        updateTableProperties(Optional.empty(), properties);
    }

    public void updateTableProperties(Optional<Transaction> transaction, Map<String, String> properties) {
        if (properties == null) {
            return;
        }

        if (table == null) {
            loadTable();
            if (table == null) {
                throw new IllegalArgumentException("Table not found: " + identifier);
            }
        }

        Map<String, String> currentProperties = table.properties();
        Map<String, String> newProperties = new HashMap<>(configuration.getIcebergTableProperties());

        // Process input properties with prefix filtering
        properties.forEach((key, value) -> {
            if (!StringUtils.isBlank(key) && (key.startsWith(TABLE_PROP_PREFIX) || key.startsWith(WRITE_PROP_PREFIX))) {
                String cleanKey = key.replaceFirst(TABLE_PROP_PREFIX, "").replaceFirst(WRITE_PROP_PREFIX, "");
                newProperties.put(cleanKey, value);
            }
        });

        for (String lakestreamManagedKey : LAKESTREAM_MANAGED_KEYS) {
            var v = properties.get(lakestreamManagedKey);
            if (v != null) {
                newProperties.put(lakestreamManagedKey, v);
            }
        }

        // Get currently managed keys from ursa.keys
        Set<String> currentlyManagedKeys = getCurrentlyManagedKeys(currentProperties);

        // Get new managed keys (keys we're about to set)
        Set<String> newManagedKeys = new HashSet<>(newProperties.keySet()
            .stream().filter(key -> !key.equalsIgnoreCase(URSA_KEYS_PROPERTY)).toList());

        // Calculate operations
        Map<String, String> toAddOrUpdate = getPropertiesToAddOrUpdate(newProperties, currentProperties);
        Set<String> toRemove = getPropertiesToRemove(currentlyManagedKeys, newManagedKeys);
        toRemove.removeAll(PRESERVED_PROPERTIES);

        // Check if any changes are needed
        if (toAddOrUpdate.isEmpty() && toRemove.isEmpty()) {
            return;
        }

        UpdateProperties updateProperties = transaction.map(Transaction::updateProperties)
            .orElse(table.updateProperties());
        log.info("Update Iceberg table properties: addedOrUpdated: {}, removed: {}", toAddOrUpdate, toRemove);

        // Apply changes
        toAddOrUpdate.forEach(updateProperties::set);
        toRemove.forEach(updateProperties::remove);

        // Update ursa.keys with the new set of managed keys
        updateUrsaKeys(updateProperties, newManagedKeys);

        updateProperties.commit();
    }

    public void commitManaged(List<ParquetFileStat> fileStats) throws LakehouseException {
        loadTable();
        logSnapshotInfo();
        AppendFiles appendFiles = table.newAppend();
        for (ParquetFileStat fileStat : fileStats) {
            log.info("add filePath: {}, partitionValues: {}, fileSize: {}", fileStat.getFilePath(),
                fileStat.getPartitionValues(), fileStat.getFileSize());
            appendFiles.appendFile(fileStat.toDataFile(table));
        }
        appendTags(appendFiles, fileStats);
        appendFiles.commit();
    }

    public void commitExternal(List<ParquetFileStat> fileStats) throws LakehouseException {
        loadTable();
        table.refresh();
        logSnapshotInfo();
        RowDelta rowDelta = table.newRowDelta();
        for (ParquetFileStat fileStat : fileStats) {
            if (fileStat == null
                    || fileStat.getWriteResults() == null
                    || fileStat.getWriteResults().isEmpty()) {
                continue;
            }
            List<WriteResult> writeResults = fileStat.getWriteResults();

            if (log.isDebugEnabled()) {
                log.debug("add writeResult: {}", writeResults);
            }

            for (WriteResult writeResult : writeResults) {
                for (DataFile dataFile : writeResult.dataFiles()) {
                    rowDelta.addRows(dataFile);
                }

                for (DeleteFile deleteFile : writeResult.deleteFiles()) {
                    rowDelta.addDeletes(deleteFile);
                }
            }
        }
        appendTags(rowDelta, fileStats);
        rowDelta.commit();
    }

    private void logSnapshotInfo() {
        if (table instanceof BaseTable bt) {
            var tableMetadata = bt.operations().current();
            var currentSnapshot = tableMetadata.currentSnapshot();
            var sid = currentSnapshot == null ? -1 : currentSnapshot.snapshotId();
            var currentSeqNumber = tableMetadata.lastSequenceNumber();
            var nextSeqNumber = bt.operations().current().nextSequenceNumber();
            log.info("Starting commit with the snapshot id: {}, current sequence number: {}, next sequence number: {}",
                    sid, currentSeqNumber, nextSeqNumber);
        } else {
            log.info("Table is not a BaseTable, cannot log snapshot info: {}", table.getClass().getName());
        }
    }

    public Iterable<Snapshot> snapshots() {
        if (table == null) {
            loadTable();
            if (table == null) {
                return null;
            }
        }
        table.refresh();
        return table.snapshots();
    }

    protected static void appendTags(SnapshotUpdate snapshotUpdate, List<ParquetFileStat> fileStats)
            throws LakehouseException {
        if (fileStats == null || fileStats.isEmpty()) {
            return;
        }

        Map<String, MessageId> tags = new HashMap<>();

        for (ParquetFileStat fileStat : fileStats) {
            Map<String, String> tagMap = fileStat.getTags();
            if (tagMap == null || tagMap.isEmpty()) {
                continue;
            }

            String topic = tagMap.get("topic");
            String streamIdStr = tagMap.get("streamId");
            String offsetStr = tagMap.get("endOffset");

            if (StringUtils.isAnyBlank(topic, streamIdStr, offsetStr)) {
                continue;
            }

            MessageId messageId =
                    new MessageId(Long.parseLong(streamIdStr), Long.parseLong(offsetStr));

            // keep max MessageId per topic
            tags.merge(topic, messageId,
                    (oldVal, newVal) -> oldVal.compareTo(newVal) < 0 ? newVal : oldVal);

        }

        if (tags.isEmpty()) {
            return;
        }

        Map<String, String> serialized =
                tags.entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> e.getValue().toString()
                        ));

        try {
            snapshotUpdate.set("lakestream.tags",
                    OBJECT_MAPPER.writeValueAsString(serialized));
        } catch (JsonProcessingException e) {
            throw new PersistTagFailedException("Failed to serialize the topic messageId map", e);
        }
    }

    // todo: Cannot delete file where some, but not all, rows match filter ref(name="userid") == 1
    public void delete(Expression expression) {
        if (table == null) {
            loadTable();
            if (table == null) {
                // no such table, skip delete
                return;
            }
        }
        table.refresh();
        table.newDelete()
            .deleteFromRowFilter(expression)
            .commit();
    }

    // todo: because there is no place to use this method for now, complete it later
    public void expireSnapshot(Duration duration) {
        if (table == null) {
            loadTable();
            if (table == null) {
                // no such table, skip
                return;
            }
        }
        table.expireSnapshots()
            .expireOlderThan(duration.toMillis())
            .commit();
    }

    public void delete(List<ParquetFileStat> fileStats) {
        loadTable();
        table.refresh();
        DeleteFiles deleteFiles = table.newDelete();
        for (ParquetFileStat fileStat : fileStats) {
            var dataFile = DataFiles.builder(table.spec())
                .withPath(configuration.getBucketPath() + fileStat.getFileFullPath())
                .withFileSizeInBytes(fileStat.getFileSize())
                .withFormat(FileFormat.PARQUET)
                .withRecordCount(fileStat.getTags().get("totalMessage") == null
                    ? 0L : Long.parseLong(fileStat.getTags().get("totalMessage")))
                .build();
            deleteFiles.deleteFile(dataFile);
        }
        deleteFiles.commit();
    }

    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        CatalogFactory.releaseCatalog(configuration, referencedCatalog);
    }

    /**
     * Get the set of keys currently managed by our system from ursa.keys property.
     */
    private Set<String> getCurrentlyManagedKeys(Map<String, String> currentProperties) {
        String ursaKeysValue = currentProperties.get(URSA_KEYS_PROPERTY);
        if (StringUtils.isBlank(ursaKeysValue)) {
            return new HashSet<>();
        }

        return Arrays.stream(ursaKeysValue.split(","))
            .map(String::trim)
            .filter(key -> !StringUtils.isBlank(key))
            .collect(Collectors.toSet());
    }

    /**
     * Get properties that need to be added or updated.
     */
    private Map<String, String> getPropertiesToAddOrUpdate(Map<String, String> newProperties,
                                                           Map<String, String> currentProperties) {
        Map<String, String> result = new HashMap<>();

        for (Map.Entry<String, String> entry : newProperties.entrySet()) {
            String key = entry.getKey();
            String newValue = entry.getValue();

            if (!currentProperties.containsKey(key) || !Objects.equals(newValue, currentProperties.get(key))) {
                result.put(key, newValue);
            }
        }

        return result;
    }

    /**
     * Get properties that need to be removed (only from keys we previously managed).
     */
    private Set<String> getPropertiesToRemove(Set<String> currentlyManagedKeys, Set<String> newManagedKeys) {
        Set<String> toRemove = new HashSet<>(currentlyManagedKeys);
        toRemove.removeAll(newManagedKeys);
        // If ursa.keys is in the set, we should not remove it, as it is used to track managed keys
        toRemove.remove(URSA_KEYS_PROPERTY);
        return toRemove;
    }

    /**
     * Update the ursa.keys property with the current set of managed keys.
     */
    private void updateUrsaKeys(UpdateProperties updateProperties, Set<String> managedKeys) {
        if (managedKeys.isEmpty()) {
            // If no keys are managed, remove the ursa.keys property
            updateProperties.remove(URSA_KEYS_PROPERTY);
        } else {
            // Sort keys for consistent ordering
            String ursaKeysValue = managedKeys.stream()
                .sorted()
                .collect(Collectors.joining(","));
            updateProperties.set(URSA_KEYS_PROPERTY, ursaKeysValue);
        }
    }

    public static Map<String, String> generateIcebergTableProperties(Map<String, String> prop) {
        if (prop == null || prop.isEmpty()) {
            return Map.of(); // immutable empty map
        }

        Map<String, String> result = new HashMap<>(prop);
        result.put(URSA_KEYS_PROPERTY, String.join(",",
            prop.keySet().stream().filter(key -> !key.equalsIgnoreCase(URSA_KEYS_PROPERTY)).toList()));
        return Map.copyOf(result); // or Collections.unmodifiableMap(result);
    }

    /**
     * Check if soft delete is enabled for schema evolution.
     * First checks table properties, then falls back to configuration.
     */
    private boolean isSoftDeleteEnabled() {
        // Check table properties first
        if (table != null) {
            String tableProperty = table.properties().get(SCHEMA_EVOLUTION_SOFT_DELETE_ENABLED);
            if (tableProperty != null) {
                return Boolean.parseBoolean(tableProperty);
            }
        }

        // Fall back to configuration
        return configuration.getProperties()
            .getProperty(SCHEMA_EVOLUTION_SOFT_DELETE_ENABLED, "true")
            .equalsIgnoreCase("true");
    }

    /**
     * Returns a copy of {@code incoming} in which every field absent from {@code current} is
     * marked optional. Fields present in both keep their nullability; structs present in both are
     * recursed so a new field nested inside an existing struct is also made optional. Field IDs,
     * names and docs are preserved.
     */
    private static org.apache.iceberg.Schema makeNewFieldsOptional(
            org.apache.iceberg.Schema current, org.apache.iceberg.Schema incoming) {
        org.apache.iceberg.types.Types.StructType merged =
            optionalizeNewFields(current.asStruct(), incoming.asStruct());
        return new org.apache.iceberg.Schema(merged.fields(), incoming.identifierFieldIds());
    }

    private static org.apache.iceberg.types.Types.StructType optionalizeNewFields(
            org.apache.iceberg.types.Types.StructType current,
            org.apache.iceberg.types.Types.StructType incoming) {
        List<org.apache.iceberg.types.Types.NestedField> result = new ArrayList<>();
        for (org.apache.iceberg.types.Types.NestedField field : incoming.fields()) {
            org.apache.iceberg.types.Types.NestedField currentField = current.field(field.name());
            if (currentField == null) {
                // Brand-new field: make it optional AND recursively optionalize every field nested
                // inside it (struct fields, list elements, map values). A REST catalog rejects
                // adding any non-nullable field, including one nested inside a newly-added
                // struct/list/map (e.g. a required 'attempt_number' inside a new optional struct).
                org.apache.iceberg.types.Type optionalType = optionalizeAllFields(field.type());
                if (!field.isOptional() || !optionalType.equals(field.type())) {
                    log.info("Adding new field '{}' as optional (recursively) for backward compatibility",
                        field.name());
                }
                result.add(org.apache.iceberg.types.Types.NestedField.optional(
                    field.fieldId(), field.name(), optionalType, field.doc()));
            } else if (field.type().isStructType() && currentField.type().isStructType()) {
                org.apache.iceberg.types.Types.StructType newStruct = optionalizeNewFields(
                    currentField.type().asStructType(), field.type().asStructType());
                result.add(org.apache.iceberg.types.Types.NestedField.of(
                    field.fieldId(), field.isOptional(), field.name(), newStruct, field.doc()));
            } else {
                result.add(field);
            }
        }
        return org.apache.iceberg.types.Types.StructType.of(result);
    }

    /**
     * Recursively marks every field nested inside {@code type} as optional: struct fields, list
     * elements and map values (map keys stay required, as Iceberg requires). Used when a brand-new
     * field is added under make-new-fields-optional so that no non-nullable field is introduced at
     * any nesting depth. Field IDs are preserved.
     */
    private static org.apache.iceberg.types.Type optionalizeAllFields(org.apache.iceberg.types.Type type) {
        if (type.isStructType()) {
            List<org.apache.iceberg.types.Types.NestedField> fields = new ArrayList<>();
            for (org.apache.iceberg.types.Types.NestedField field : type.asStructType().fields()) {
                fields.add(org.apache.iceberg.types.Types.NestedField.optional(
                    field.fieldId(), field.name(), optionalizeAllFields(field.type()), field.doc()));
            }
            return org.apache.iceberg.types.Types.StructType.of(fields);
        }
        if (type.isListType()) {
            org.apache.iceberg.types.Types.ListType list = type.asListType();
            return org.apache.iceberg.types.Types.ListType.ofOptional(
                list.elementId(), optionalizeAllFields(list.elementType()));
        }
        if (type.isMapType()) {
            org.apache.iceberg.types.Types.MapType map = type.asMapType();
            return org.apache.iceberg.types.Types.MapType.ofOptional(
                map.keyId(), map.valueId(),
                optionalizeAllFields(map.keyType()), optionalizeAllFields(map.valueType()));
        }
        return type;
    }

    public int updateTableSchemaIfNeeded(@NotNull Schema newSchema) throws SchemaEvolutionException {
        return updateTableSchemaIfNeeded(Optional.empty(), newSchema);
    }

    /**
     * Update the table schema if it differs from the current schema. Returns the new schema ID if updated.
     * If this operation is part of a larger transaction, the returned schema ID is only valid after the
     * transaction is committed successfully. If the transaction commit fails, you need to fetch the latest
     * schema ID from the table again.
     *
     * @param transaction
     * @param newSchema
     * @return the new schema ID after update, or current schema ID if no update was needed
     */
    public int updateTableSchemaIfNeeded(Optional<Transaction> transaction,
                                         @NotNull org.apache.iceberg.Schema newSchema) throws SchemaEvolutionException {
        if (newSchema == null) {
            throw new IllegalArgumentException("New schema cannot be null");
        }
        // Load the table if not already loaded
        loadTable();
        // Refresh table to get latest metadata
        table.refresh();

        org.apache.iceberg.Schema currentSchema = table.schema();

        // Check if schemas are the same
        if (schemasAreEqual(currentSchema, newSchema)) {
            log.debug("Schema is already up to date for table: {}", identifier);
            return currentSchema.schemaId();
        }

        log.info("Updating table schema from {} to {} for table: {}",
                currentSchema, newSchema, identifier);

        // Let Iceberg handle ALL validation (it's comprehensive!)
        Schema reassignedSchema = TypeUtil.reassignOrRefreshIds(newSchema, currentSchema);

        if (configuration.makeNewFieldsOptionalOnEvolution()) {
            // The "required, but is missing" check in TypeUtil.validateSchema is NOT gated by
            // checkNullability, so new required fields must be optional in the schema we validate.
            reassignedSchema = makeNewFieldsOptional(currentSchema, reassignedSchema);
        }

        validateSchema(currentSchema, reassignedSchema, configuration.checkIcebergNullability(), false);

        boolean hasV3Schema = IcebergTableUtils.containsV3Types(reassignedSchema);
        upgradeTableFormatIfNeeded(hasV3Schema, transaction, reassignedSchema);

        // Use Iceberg's schema evolution to update the schema
        var updateSchema = transaction.map(Transaction::updateSchema).orElse(table.updateSchema());

        // Apply changes from the (optionalized, when the flag is on) reassigned schema, so the
        // committed schema matches exactly what validateSchema checked above.
        applySchemaChanges(updateSchema, currentSchema, reassignedSchema);

        var updatedSchema = updateSchema.apply();

        if (currentSchema.sameSchema(updatedSchema)) {
            updateSchema.commit();
            return currentSchema.schemaId();
        }

        validateSchema(currentSchema, updatedSchema, configuration.checkIcebergNullability(),
                configuration.checkIcebergOrdering());

        // Commit the schema changes
        updateSchema.commit();

        // Update name mapping if not Unity Catalog
        updateNameMappingAfterSchemaChange(transaction);

        log.info("Schema updated successfully for table: {}", identifier);

        return currentSchema.schemaId() + 1;
    }

    private void validateSchema(org.apache.iceberg.Schema currentSchema,
                                org.apache.iceberg.Schema newSchema,
                                boolean checkNullability,
                                boolean checkOrder) throws SchemaEvolutionException {
        try {
            TypeUtil.validateSchema("data", newSchema, currentSchema, checkNullability, checkOrder);
        } catch (IllegalArgumentException e) {
            log.error("Schema validation failed for table: {}. Current schema: {}, New schema: {}",
                    identifier, currentSchema, newSchema, e);
            throw new SchemaEvolutionException(e.getMessage(), e);
        }
    }


    /**
     * Check if two schemas are functionally equal.
     */
    private boolean schemasAreEqual(org.apache.iceberg.Schema current, org.apache.iceberg.Schema newSchema) {
        return schemasAreEqual(current.asStruct(), newSchema.asStruct());
    }

    /**
     * Recursively check if two struct types are equal.
     */
    private boolean schemasAreEqual(org.apache.iceberg.types.Types.StructType current,
                                    org.apache.iceberg.types.Types.StructType newStruct) {
        if (current.fields().size() != newStruct.fields().size()) {
            return false;
        }

        for (int i = 0; i < current.fields().size(); i++) {
            org.apache.iceberg.types.Types.NestedField currentField = current.fields().get(i);
            org.apache.iceberg.types.Types.NestedField newField = newStruct.fields().get(i);

            if (!Objects.equals(currentField.name(), newField.name())
                    || currentField.isOptional() != newField.isOptional()) {
                return false;
            }

            // Recursively check nested structures
            if (!typesAreEqual(currentField.type(), newField.type())) {
                return false;
            }
        }

        return true;
    }

    /**
     * Recursively check if two types are equal.
     */
    private boolean typesAreEqual(org.apache.iceberg.types.Type current, org.apache.iceberg.types.Type newType) {
        if (current.typeId() != newType.typeId()) {
            return false;
        }

        switch (current.typeId()) {
            case STRUCT:
                return schemasAreEqual((org.apache.iceberg.types.Types.StructType) current,
                        (org.apache.iceberg.types.Types.StructType) newType);
            case LIST:
                org.apache.iceberg.types.Types.ListType currentList = (org.apache.iceberg.types.Types.ListType) current;
                org.apache.iceberg.types.Types.ListType newList = (org.apache.iceberg.types.Types.ListType) newType;
                return currentList.isElementOptional() == newList.isElementOptional()
                        && typesAreEqual(currentList.elementType(), newList.elementType());
            case MAP:
                org.apache.iceberg.types.Types.MapType currentMap = (org.apache.iceberg.types.Types.MapType) current;
                org.apache.iceberg.types.Types.MapType newMap = (org.apache.iceberg.types.Types.MapType) newType;
                return currentMap.isValueOptional() == newMap.isValueOptional()
                        && typesAreEqual(currentMap.keyType(), newMap.keyType())
                        && typesAreEqual(currentMap.valueType(), newMap.valueType());
            default:
                return Objects.equals(current, newType);
        }
    }

    /**
     * Apply schema changes by comparing current and new schemas.
     */
    private void applySchemaChanges(org.apache.iceberg.UpdateSchema updateSchema,
                                    org.apache.iceberg.Schema currentSchema,
                                    org.apache.iceberg.Schema newSchema) {
        applyStructChanges(updateSchema, currentSchema.asStruct(), newSchema.asStruct(), "");
    }

    private void applyStructChanges(org.apache.iceberg.UpdateSchema updateSchema,
                                    org.apache.iceberg.types.Types.StructType currentStruct,
                                    org.apache.iceberg.types.Types.StructType newStruct,
                                    String parentPath) {
        // Get current and new field names for comparison
        Map<String, org.apache.iceberg.types.Types.NestedField> currentFieldMap = currentStruct.fields().stream()
                .collect(Collectors.toMap(
                        org.apache.iceberg.types.Types.NestedField::name,
                        field -> field
                ));

        Map<String, org.apache.iceberg.types.Types.NestedField> newFieldMap = newStruct.fields().stream()
                .collect(Collectors.toMap(
                        org.apache.iceberg.types.Types.NestedField::name,
                        field -> field
                ));

        Set<String> currentFieldNames = currentFieldMap.keySet();
        Set<String> newFieldNames = newFieldMap.keySet();

        // Add new fields (all new fields must be optional for backward compatibility)
        for (org.apache.iceberg.types.Types.NestedField newField : newStruct.fields()) {
            if (!currentFieldNames.contains(newField.name())) {
                String fieldPath = parentPath.isEmpty() ? newField.name() : parentPath + "." + newField.name();

                if (newField.isOptional()) {
                    log.info("Adding new optional field: {} with type: {}", fieldPath, newField.type());
                    if (parentPath.isEmpty()) {
                        updateSchema.addColumn(newField.name(), newField.type());
                    } else {
                        updateSchema.addColumn(parentPath, newField.name(), newField.type());
                    }
                } else {
                    log.warn("Cannot add required field '{}' to existing table - "
                            + "Iceberg only supports adding optional fields. "
                            + "Adding as optional field instead for backward compatibility.", fieldPath);
                    if (parentPath.isEmpty()) {
                        updateSchema.addColumn(newField.name(), newField.type());
                    } else {
                        updateSchema.addColumn(parentPath, newField.name(), newField.type());
                    }
                }
            }
        }

        // Handle field type promotions and nullability changes for existing fields
        for (org.apache.iceberg.types.Types.NestedField newField : newStruct.fields()) {
            org.apache.iceberg.types.Types.NestedField currentField = currentFieldMap.get(newField.name());

            if (currentField != null) {
                String fieldPath = parentPath.isEmpty() ? newField.name() : parentPath + "." + newField.name();

                // Check for type changes
                if (!typesAreEqual(currentField.type(), newField.type())) {
                    handleTypeEvolution(updateSchema, currentField, newField, fieldPath, parentPath);
                }

                // Make required field optional if needed
                if (!currentField.isOptional() && newField.isOptional()) {
                    log.info("Making field optional: {}", fieldPath);
                    if (parentPath.isEmpty()) {
                        updateSchema.makeColumnOptional(newField.name());
                    } else {
                        updateSchema.makeColumnOptional(fieldPath);
                    }
                } else if (currentField.isOptional() && !newField.isOptional()) {
                    // This shouldn't happen if Iceberg validation passed
                    log.warn("Cannot make optional field required: {} (should have been caught by validation)",
                            fieldPath);
                }
            }
        }

        // Handle deleted fields based on soft delete setting
        boolean softDeleteEnabled = isSoftDeleteEnabled();
        for (org.apache.iceberg.types.Types.NestedField currentField : currentStruct.fields()) {
            if (!newFieldNames.contains(currentField.name())) {
                String fieldPath = parentPath.isEmpty() ? currentField.name() : parentPath + "." + currentField.name();

                if (softDeleteEnabled) {
                    log.info("Field '{}' not found in new schema, marking as optional (soft delete)", fieldPath);
                    // Only mark as optional if it's currently required
                    if (!currentField.isOptional()) {
                        if (parentPath.isEmpty()) {
                            updateSchema.makeColumnOptional(currentField.name());
                        } else {
                            updateSchema.makeColumnOptional(fieldPath);
                        }
                    }
                    // Field is effectively "deleted" but remains in schema for backward compatibility
                } else {
                    log.info("Field '{}' not found in new schema, performing hard delete", fieldPath);

                    if (!currentField.isOptional()) {
                        log.warn("Attempting to hard delete required field '{}'. "
                                        + "This may fail if the field contains data. "
                                        + "Consider enabling soft delete mode "
                                        + "(schema.evolution.soft-delete.enabled=true).",
                                fieldPath);
                    }

                    if (parentPath.isEmpty()) {
                        updateSchema.deleteColumn(currentField.name());
                    } else {
                        updateSchema.deleteColumn(fieldPath);
                    }
                    log.info("Successfully deleted field: {}", fieldPath);
                }
            }
        }
    }

    private void handleTypeEvolution(org.apache.iceberg.UpdateSchema updateSchema,
                                     org.apache.iceberg.types.Types.NestedField currentField,
                                     org.apache.iceberg.types.Types.NestedField newField,
                                     String fieldPath,
                                     String parentPath) {
        org.apache.iceberg.types.Type currentType = currentField.type();
        org.apache.iceberg.types.Type newType = newField.type();

        org.apache.iceberg.types.Type.TypeID currentTypeId = currentType.typeId();
        org.apache.iceberg.types.Type.TypeID newTypeId = newType.typeId();

        // Check if type category changed (e.g., struct → list, primitive → map, etc.)
        // For primitives, we need to check if promotion is allowed instead of just comparing TypeIDs
        if (currentType.isPrimitiveType() && newType.isPrimitiveType()) {
            // Both are primitives - check if promotion is allowed
            if (TypeUtil.isPromotionAllowed(currentType.asPrimitiveType(), newType.asPrimitiveType())) {
                log.info("Promoting field type: {} from {} to {}", fieldPath, currentType, newType);
                if (parentPath.isEmpty()) {
                    updateSchema.updateColumn(newField.name(), newType.asPrimitiveType());
                } else {
                    updateSchema.updateColumn(fieldPath, newType.asPrimitiveType());
                }
            } else {
                log.warn("Incompatible primitive type change for field {}: {} to {} (promotion not allowed)",
                        fieldPath, currentType, newType);
            }
            return;
        }

        // For non-primitives, check if type category changed
        if (currentTypeId != newTypeId) {
            handleTypeCategoryChange(currentType, newType, fieldPath);
            return;
        }

        // Same type category - handle evolution within that category
        switch (currentTypeId) {
            case STRUCT:
                log.info("Recursively handling struct evolution for field: {}", fieldPath);
                applyStructChanges(updateSchema,
                        (org.apache.iceberg.types.Types.StructType) currentType,
                        (org.apache.iceberg.types.Types.StructType) newType,
                        fieldPath);
                break;

            case LIST:
                handleListEvolution(updateSchema, currentType, newType, fieldPath);
                break;

            case MAP:
                handleMapEvolution(updateSchema, currentType, newType, fieldPath);
                break;

            default:
                // This shouldn't be reached now that primitives are handled above
                log.warn("Unexpected type category for field {}: {}", fieldPath, currentTypeId);
                break;
        }
    }

    private void handleTypeCategoryChange(org.apache.iceberg.types.Type currentType,
                                          org.apache.iceberg.types.Type newType,
                                          String fieldPath) {
        log.warn("Type category change detected for field {}: {} ({}) to {} ({}). "
                        + "Iceberg does not support changing between different type categories "
                        + "(e.g., struct ↔ list, primitive ↔ map, etc.). "
                        + "This field cannot be automatically evolved. "
                        + "Options: (1) Enable soft delete mode to mark old field as optional "
                        + "and add new field with different name, "
                        + "(2) Drop and recreate the table, or (3) Handle at application level with schema-on-read.",
                fieldPath,
                currentType.typeId(),
                currentType,
                newType.typeId(),
                newType);
    }

    private void handleListEvolution(org.apache.iceberg.UpdateSchema updateSchema,
                                     org.apache.iceberg.types.Type currentType,
                                     org.apache.iceberg.types.Type newType,
                                     String fieldPath) {
        org.apache.iceberg.types.Types.ListType currentList = (org.apache.iceberg.types.Types.ListType) currentType;
        org.apache.iceberg.types.Types.ListType newList = (org.apache.iceberg.types.Types.ListType) newType;

        org.apache.iceberg.types.Type currentElementType = currentList.elementType();
        org.apache.iceberg.types.Type newElementType = newList.elementType();

        log.debug("handleListEvolution called for field: {}, currentElement: {}, newElement: {}",
                fieldPath, currentElementType, newElementType);

        // If element types are the same, nothing to do
        if (typesAreEqual(currentElementType, newElementType)) {
            log.debug("List element types are equal for field: {}, skipping evolution", fieldPath);
            return;
        }

        org.apache.iceberg.types.Type.TypeID currentElementTypeId = currentElementType.typeId();
        org.apache.iceberg.types.Type.TypeID newElementTypeId = newElementType.typeId();

        // Check if element type category changed
        if (currentElementTypeId != newElementTypeId) {
            log.warn("List element type category changed for field {}: {} to {}. "
                            + "Cannot evolve list when element type category changes. "
                            + "Consider using soft delete mode.",
                    fieldPath, currentElementType, newElementType);
            return;
        }

        log.info("List element type evolution detected for field: {} from {} to {}",
                fieldPath, currentElementType, newElementType);

        String elementPath = fieldPath + ".element";

        // Handle based on element type category
        switch (currentElementTypeId) {
            case STRUCT:
                log.info("Recursively handling struct evolution for list element at: {}", elementPath);
                applyStructChanges(updateSchema,
                        (org.apache.iceberg.types.Types.StructType) currentElementType,
                        (org.apache.iceberg.types.Types.StructType) newElementType,
                        elementPath);
                break;

            case LIST:
                log.info("Recursively handling nested list evolution at: {}", elementPath);
                handleListEvolution(updateSchema, currentElementType, newElementType, elementPath);
                break;

            case MAP:
                log.info("Recursively handling map evolution for list element at: {}", elementPath);
                handleMapEvolution(updateSchema, currentElementType, newElementType, elementPath);
                break;

            default:
                // Primitive types
                if (currentElementType.isPrimitiveType() && newElementType.isPrimitiveType()
                        && TypeUtil.isPromotionAllowed(currentElementType.asPrimitiveType(),
                        newElementType.asPrimitiveType())) {
                    log.info("Promoting list element type: {} from {} to {}",
                            fieldPath, currentElementType, newElementType);
                    updateSchema.updateColumn(elementPath, newElementType.asPrimitiveType());
                } else {
                    log.warn("Unsupported list element type change for field {}: {} to {}. "
                                    + "Consider using soft delete mode to handle this change.",
                            fieldPath, currentElementType, newElementType);
                }
                break;
        }
    }

    private void handleMapEvolution(org.apache.iceberg.UpdateSchema updateSchema,
                                    org.apache.iceberg.types.Type currentType,
                                    org.apache.iceberg.types.Type newType,
                                    String fieldPath) {
        org.apache.iceberg.types.Types.MapType currentMap = (org.apache.iceberg.types.Types.MapType) currentType;
        org.apache.iceberg.types.Types.MapType newMap = (org.apache.iceberg.types.Types.MapType) newType;

        org.apache.iceberg.types.Type currentKeyType = currentMap.keyType();
        org.apache.iceberg.types.Type newKeyType = newMap.keyType();
        org.apache.iceberg.types.Type currentValueType = currentMap.valueType();
        org.apache.iceberg.types.Type newValueType = newMap.valueType();

        boolean keyChanged = !typesAreEqual(currentKeyType, newKeyType);
        boolean valueChanged = !typesAreEqual(currentValueType, newValueType);

        if (!keyChanged && !valueChanged) {
            return;
        }

        log.info("Map type evolution detected for field: {}", fieldPath);

        // Handle key type changes
        if (keyChanged) {
            // Map keys must be primitives in Iceberg
            if (!currentKeyType.isPrimitiveType() || !newKeyType.isPrimitiveType()) {
                log.warn("Map key type must be primitive for field {}: current={}, new={}",
                        fieldPath, currentKeyType, newKeyType);
            } else if (currentKeyType.typeId() != newKeyType.typeId()) {
                log.warn("Map key type category changed for field {}: {} to {}. "
                                + "Cannot change key type category.",
                        fieldPath, currentKeyType, newKeyType);
            } else if (TypeUtil.isPromotionAllowed(currentKeyType.asPrimitiveType(),
                    newKeyType.asPrimitiveType())) {
                log.info("Promoting map key type: {} from {} to {}",
                        fieldPath, currentKeyType, newKeyType);
                updateSchema.updateColumn(fieldPath + ".key", newKeyType.asPrimitiveType());
            } else {
                log.warn("Unsupported map key type change for field {}: {} to {}",
                        fieldPath, currentKeyType, newKeyType);
            }
        }

        // Handle value type changes
        if (valueChanged) {
            org.apache.iceberg.types.Type.TypeID currentValueTypeId = currentValueType.typeId();
            org.apache.iceberg.types.Type.TypeID newValueTypeId = newValueType.typeId();

            // Check if value type category changed
            if (currentValueTypeId != newValueTypeId) {
                log.warn("Map value type category changed for field {}: {} to {}. "
                                + "Cannot evolve map when value type category changes. "
                                + "Consider using soft delete mode.",
                        fieldPath, currentValueType, newValueType);
                return;
            }

            String valuePath = fieldPath + ".value";

            // Handle based on value type category
            switch (currentValueTypeId) {
                case STRUCT:
                    log.info("Recursively handling struct evolution for map value at: {}", valuePath);
                    applyStructChanges(updateSchema,
                            (org.apache.iceberg.types.Types.StructType) currentValueType,
                            (org.apache.iceberg.types.Types.StructType) newValueType,
                            valuePath);
                    break;

                case LIST:
                    log.info("Recursively handling list evolution for map value at: {}", valuePath);
                    handleListEvolution(updateSchema, currentValueType, newValueType, valuePath);
                    break;

                case MAP:
                    log.info("Recursively handling nested map evolution at: {}", valuePath);
                    handleMapEvolution(updateSchema, currentValueType, newValueType, valuePath);
                    break;

                default:
                    // Primitive types
                    if (currentValueType.isPrimitiveType() && newValueType.isPrimitiveType()
                            && TypeUtil.isPromotionAllowed(currentValueType.asPrimitiveType(),
                            newValueType.asPrimitiveType())) {
                        log.info("Promoting map value type: {} from {} to {}",
                                fieldPath, currentValueType, newValueType);
                        updateSchema.updateColumn(valuePath, newValueType.asPrimitiveType());
                    } else {
                        log.warn("Unsupported map value type change for field {}: {} to {}. "
                                        + "Consider using soft delete mode to handle this change.",
                                fieldPath, currentValueType, newValueType);
                    }
                    break;
            }
        }
    }

    /**
     * Update name mapping after schema change (similar to createIfAbsent logic).
     */
    private void updateNameMappingAfterSchemaChange(Optional<Transaction> transaction) {
        String catalogBackendType = configuration.getIcebergCatalogBackendType(catalogName).toString();
        if (!ICEBERG_CATALOG_TYPE_UNITYCATALOG.equalsIgnoreCase(catalogBackendType)
                && !ICEBERG_CATALOG_TYPE_HORIZON.equalsIgnoreCase(catalogBackendType)) {

            NameMapping nameMapping = MappingUtil.create(table.schema());
            String schemaMapping = NameMappingParser.toJson(nameMapping);

            transaction.map(Transaction::updateProperties).orElse(table.updateProperties())
                .set("schema.name-mapping.default", schemaMapping)
                .commit();

            log.info("Updated table properties with new schema name mapping for table: {}", identifier);
        }
    }

    protected void upgradeTableFormatIfNeeded(boolean hasV3Schema, Optional<Transaction> transaction, Schema schema)
            throws SchemaEvolutionException {
        if (hasV3Schema && table instanceof BaseTable bt) {
            TableMetadata currentMetadata = bt.operations().current();
            if (currentMetadata.formatVersion() < 3) {
                if (!configuration.isAllowIcebergV3()) {
                    throw new SchemaEvolutionException("Cannot evolve to schema with V3 types on a table "
                            + "with format version < 3 without explicit upgrade permission. Current format version: "
                            + currentMetadata.formatVersion() + ", attempted schema: " + schema);
                } else {
                    log.info("Upgrading table format version to 3 for table: {}", identifier);
                    transaction.map(Transaction::updateProperties).orElse(table.updateProperties())
                            .set(FORMAT_VERSION, "3")
                            .commit();
                    log.info("Successfully upgraded table format version to 3 for table: {}", identifier);
                }
            }
        }
    }

    Transaction startTransaction() {
        return table.newTransaction();
    }

    void commitTransaction(Transaction transaction) {
        transaction.commitTransaction();
    }

    /**
     * Evolve schema with a specific version ID.
     * The version ID here means to the topic schema version ID in the schema registry.
     *
     * @param versionId
     *          the topic schema version ID to evolve to. It must be unique and greater than the current max version ID.
     * @param schema
     *          the new schema to evolve to.
     */
    public void evolveSchemaWithVersion(long versionId, Schema schema)
        throws SchemaMappingException, SchemaEvolutionException {

        table.refresh();

        var schemaMapping = getSchemaMapping();
        if (schemaMapping.containsKey(versionId)) {
            if (schemaMapping.get(versionId).equals(-1)) {
                throw new SchemaEvolutionException(
                    "The evolution of schema version ID " + versionId + " has failed before.");
            }
            log.info("Schema version {} already exists, skipping evolution for table: {}", versionId, identifier);
            return;
        }

        var txn = startTransaction();
        SchemaEvolutionException deferedException = null;
        var newSchemaId = -1;
        try {
            newSchemaId = updateTableSchemaIfNeeded(Optional.of(txn), schema);
        } catch (SchemaEvolutionException e) {
            deferedException = e;
        }
        schemaMapping.put(versionId, newSchemaId);
        saveSchemaMapping(Optional.of(txn), schemaMapping);
        commitTransaction(txn);

        if (deferedException != null) {
            throw deferedException;
        }
    }

    public Optional<Schema> getSchemaByVersion(long versionId) throws SchemaMappingException {
        loadTable();
        table.refresh();
        Map<Long, Integer> schemaMapping = getSchemaMapping();
        var id = schemaMapping.get(versionId);
        if (id == null || id == -1) {
            return Optional.empty();
        }
        return Optional.ofNullable(table.schemas().get(id));
    }

    public @NotNull Map<Long, Integer> getSchemaMapping() throws SchemaMappingException {
        loadTable();
        table.refresh();
        var schemaInfo = table.properties().get(LAKESTREAM_SCHEMA_MAPPING);
        if (StringUtils.isBlank(schemaInfo)) {
            return new HashMap<>();
        }
        try {
            return OBJECT_MAPPER.readValue(schemaInfo, new TypeReference<Map<Long, Integer>>() {});
        } catch (JsonProcessingException e) {
            throw new SchemaMappingException("Failed to get schema mapping for table: " + identifier, e);
        }
    }

    void saveSchemaMapping(Optional<Transaction> transaction, @NotNull Map<Long, Integer> schemaMapping)
        throws SchemaMappingException {
        String schemaInfo = null;
        try {
            schemaInfo = OBJECT_MAPPER.writeValueAsString(schemaMapping);
        } catch (JsonProcessingException e) {
            throw new SchemaMappingException("Failed to save schema mapping for table: " + identifier, e);
        }
        updateTableProperties(transaction, Map.of(LAKESTREAM_SCHEMA_MAPPING, schemaInfo));
    }

    public static TableIdentifier getTableIdentifierByTopic(String topic) {
        var partitionedTopicName = TopicName.getPartitionedTopicName(topic);
        var icebergNs = Namespace.of(partitionedTopicName.getNamespace());
        var tableName = partitionedTopicName.getLocalName();
        return TableIdentifier.of(icebergNs, tableName);
    }

    public static long getLatestMetadataSize(Table table) {
        table.refresh();
        try {
            if (table instanceof HasTableOperations tableOperations) {
                TableMetadata tableMetadata = tableOperations.operations().current();
                if (tableMetadata == null) {
                    return 0;
                }

                return table.io().newInputFile(tableMetadata.metadataFileLocation()).getLength();
            }
            return 0;
        } catch (Throwable e) { // catch all exceptions to avoid affecting main flow
            log.warn("Failed to get latest metadata size for table: {}", table.name(), e);
        }

        return 0;
    }

    private static void saveNameMapping(Table table,
                                        LakehouseConfiguration configuration,
                                        Optional<String> catalogName) {
        // if the catalog is unity catalog, we need not to set the schema name mapping
        // https://github.com/lakestream-io/ursa-storage/issues/1054
        String catalogType = configuration.getIcebergCatalogBackendType(catalogName).toString();
        if (!ICEBERG_CATALOG_TYPE_UNITYCATALOG.equalsIgnoreCase(catalogType)
                && !ICEBERG_CATALOG_TYPE_HORIZON.equalsIgnoreCase(catalogType)) {
            NameMapping nameMapping = MappingUtil.create(table.schema());
            String schemaMapping = NameMappingParser.toJson(nameMapping);
            table.updateProperties()
                    .set("schema.name-mapping.default", schemaMapping)
                    .commit();
            log.info("Update table properties with schema name mapping: {}",
                    table.properties().get("schema.name-mapping.default"));
        }
    }
}
