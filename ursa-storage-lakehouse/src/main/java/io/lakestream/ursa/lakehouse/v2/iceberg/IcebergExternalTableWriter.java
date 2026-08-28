/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.iceberg;

import com.google.common.annotations.VisibleForTesting;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.LakehouseOptException;
import io.lakestream.ursa.exception.MessageSerDeException;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig;
import io.lakestream.ursa.lakehouse.iceberg.IcebergTable;
import io.lakestream.ursa.lakehouse.iceberg.Operation;
import io.lakestream.ursa.lakehouse.iceberg.RecordWrapper;
import io.lakestream.ursa.lakehouse.iceberg.TableOptions;
import io.lakestream.ursa.lakehouse.iceberg.Utilities;
import io.lakestream.ursa.lakehouse.v2.AbstractLakehouseWriter;
import io.lakestream.ursa.lakehouse.v2.IWriteResult;
import io.lakestream.ursa.materialization.serde.EntrySerdeFactory;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.MaterializationRecord;
import io.lakestream.ursa.materialization.serde.TableSchemaService;
import io.lakestream.ursa.metrics.InstrumentProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

@Slf4j
public class IcebergExternalTableWriter extends AbstractLakehouseWriter {

    private final IcebergSinkConfig icebergSinkConfig;
    private Map<Integer, TaskWriter<Record>> taskWriters;
    @VisibleForTesting
    @Getter
    private IcebergTable icebergTable;
    private IcebergTableSchemaService icebergTableSchemaService;
    private boolean createdTable = false;
    private boolean isSchemaEvolutionEnabled = false;
    protected final EntrySerdeFactory.SerdeType serializeType;
    private Schema reassignedTableSchema = null;

    @VisibleForTesting
    public IcebergExternalTableWriter(String topic, EntrySerdeFactory entrySerdeFactory,
                               LakehouseConfiguration configuration) {
        this(topic, entrySerdeFactory, configuration, InstrumentProvider.NOOP);
    }

    public IcebergExternalTableWriter(String topic, EntrySerdeFactory entrySerdeFactory,
                                      LakehouseConfiguration config, InstrumentProvider provider) {
        this(topic, topic, entrySerdeFactory, config, provider);
    }

    public IcebergExternalTableWriter(String topic, String schemaTopic,
                                      EntrySerdeFactory entrySerdeFactory,
                                      LakehouseConfiguration config, InstrumentProvider provider) {
        super(topic, schemaTopic, entrySerdeFactory, config, provider);
        this.serializeType = getSerializeType();
        this.encoder = entrySerdeFactory.getEncoder(serializeType);
        this.icebergSinkConfig = new IcebergSinkConfig(config.getProperties());
        this.isSchemaEvolutionEnabled = config.isSchemaEvolutionEnabled();
        this.icebergTable = new IcebergTable(config, IcebergTable.getTableIdentifierByTopic(topic));
        if (isSchemaEvolutionEnabled) {
            this.icebergTableSchemaService = new IcebergTableSchemaService(icebergTable, config);
        }
        this.taskWriters = new ConcurrentHashMap<>();
    }

    private EntrySerdeFactory.SerdeType getSerializeType() {
        return EntrySerdeFactory.SerdeType.KAFKA_ICEBERG;
    }

    @Override
    protected void beforeWrite(MaterializationRecord<Object> entry) throws LakehouseOptException {
        if (!(entry.record() instanceof Record)) {
            throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_WRITE_ERROR,
                "Invalid records written to the iceberg. Only iceberg Record is supported");
        }
        Record record = (Record) entry.record();
        try {
            if (!createdTable) {
                createIcebergTable(record.struct().asSchema());
                icebergTable.updateTablePartitionSpecIfNeed();
                createdTable = true;
            }
        } catch (Throwable throwable) {
            if (throwable instanceof LakehouseOptException loe) {
                throw loe;
            }
            throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_CREATE_TABLE_ERROR, throwable);
        }
    }

    @Override
    protected void doWrite(AtomicBoolean writeNumberOfMessagesForThisEntry, GenericEntry genericEntry,
                           MaterializationRecord<Object> objectLakehouseEntry, long writeStartTime)
        throws LakehouseOptException, MessageSerDeException {

        Record record = (Record) objectLakehouseEntry.record();
        Schema schema;

        try {
            if (isSchemaEvolutionEnabled) {
                schema = record.struct().asSchema();
            } else {
                schema = icebergTable.getTable().schema();
                if (reassignedTableSchema == null) {
                    reassignedTableSchema = TypeUtil.reassignOrRefreshIds(schema, record.struct().asSchema());
                }

                if (!reassignedTableSchema.sameSchema(record.struct().asSchema())) {
                    throw new MessageSerDeException(ExceptionCode.MESSAGE_SCHEMA_INCOMPATIBLE,
                            "Schema evolution is disabled, but the record schema does not match the table schema. "
                                    + "Record schema: " + record.struct().asSchema()
                                    + ", Table schema: " + reassignedTableSchema);
                }
            }
            TaskWriter<Record> taskWriter = taskWriters.computeIfAbsent(record.struct().hashCode(),
                k -> Utilities.createTableWriter(icebergTable.getTable(), schema, partition, icebergSinkConfig));
            if (StringUtils.isNotEmpty(icebergSinkConfig.getCdcField())) {
                Operation op = extractCdcOperation(record, icebergSinkConfig.getCdcField());
                taskWriter.write(new RecordWrapper(record, op));
            } else {
                taskWriter.write(record);
            }
        } catch (MessageSerDeException me) {
            throw me;
        } catch (Throwable e) {
            if (isRecordValueIncompatibleWriteError(e, record)) {
                throw new MessageSerDeException(ExceptionCode.MESSAGE_SERIALIZE_TO_LAKEHOUSE_ERROR,
                    "Failed to write record to Iceberg table because the record value is incompatible with the "
                        + "expected struct schema. Record schema: " + record.struct().asSchema(), e);
            }
            throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_WRITE_ERROR, e);
        }
    }

    private boolean isRecordValueIncompatibleWriteError(Throwable throwable, Record record) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof NullPointerException && current.getMessage() != null
                && (isStructLikeNullWriteError(current) || isNullValueWriteError(current, record))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private boolean isStructLikeNullWriteError(Throwable throwable) {
        String message = throwable.getMessage();
        return message.contains("StructLike.get") && message.contains("\"struct\" is null");
    }

    private boolean isNullValueWriteError(Throwable throwable, Record record) {
        String message = throwable.getMessage();
        if (!message.contains("\"value\" is null")) {
            return false;
        }
        return hasMapFieldWithNullEntry(record) || hasRequiredFieldWithNullValue(record);
    }

    private boolean hasMapFieldWithNullEntry(Record record) {
        for (Types.NestedField field : record.struct().fields()) {
            if (!field.type().isMapType()) {
                continue;
            }
            Object value = record.getField(field.name());
            if (value instanceof Map<?, ?> map && (map.containsKey(null) || map.containsValue(null))) {
                return true;
            }
        }
        return false;
    }

    private boolean hasRequiredFieldWithNullValue(Record record) {
        for (Types.NestedField field : record.struct().fields()) {
            if (field.isRequired() && record.getField(field.name()) == null) {
                return true;
            }
        }
        return false;
    }

    @Override
    public TableSchemaService getLakehouseTableSchemaService() {
        return icebergTableSchemaService;
    }

    @Override
    public List<IWriteResult> close() throws LakehouseOptException {
        logCloseWarnings();
        try {
            if (taskWriters == null || taskWriters.isEmpty()) {
                log.warn("No data written to iceberg table for topic: {}, partition: {}", topic, partition);
                return Collections.emptyList();
            }

            List<IWriteResult> writeResults = new ArrayList<>();
            for (TaskWriter<Record> taskWriter : taskWriters.values()) {
                try (taskWriter) {
                    WriteResult writeResult = taskWriter.complete();
                    if (writeResult == null || (writeResult.dataFiles().length == 0
                                                && writeResult.deleteFiles().length == 0)) {
                        continue;
                    }
                    writeResults.add(new IcebergWriteResult(writeResult));
                }
            }
            return writeResults;
        } catch (IOException e) {
            log.error("Failed to close iceberg writer", e);
            throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_WRITE_ERROR, "Failed to close iceberg writer", e);
        } finally {
            if (icebergTable != null) {
                icebergTable.close();
                icebergTable = null; // prevent double-close
            }
        }
    }

    private Operation extractCdcOperation(Record recordValue, String cdcField) {
        Object opValue = Utilities.extractFromRecordValue(recordValue, cdcField);

        if (opValue == null) {
            return Operation.INSERT;
        }

        String opStr = opValue.toString().trim().toUpperCase(Locale.ROOT);
        if (opStr.isEmpty()) {
            return Operation.INSERT;
        }

        return switch (opStr.charAt(0)) {
            case 'U' -> Operation.UPDATE;
            case 'D' -> Operation.DELETE;
            default -> Operation.INSERT;
        };
    }

    private void createIcebergTable(Schema schema) throws LakehouseOptException {
        try {
            TableOptions options = TableOptions.builder()
                .schema(schema)
                .partitionKey(configuration.getPartitionKey())
                .identifierFields(configuration.getIdentifierFields())
                .properties(configuration.getIcebergTableProperties())
                .build();
            icebergTable.create(options);
        } catch (Throwable throwable) {
            throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_CREATE_TABLE_ERROR,
                "Failed to create iceberg table for topic: " + topic, throwable);
        }
    }
}
