/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.delta;

import com.google.common.annotations.VisibleForTesting;
import io.delta.kernel.types.StructType;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.LakehouseOptException;
import io.lakestream.ursa.exception.MessageSerDeException;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.delta.ExternalDeltaTable;
import io.lakestream.ursa.lakehouse.delta.ExternalDeltaTableFactory;
import io.lakestream.ursa.lakehouse.delta.GenericRow;
import io.lakestream.ursa.lakehouse.delta.ParquetRowWriter;
import io.lakestream.ursa.lakehouse.utils.TopicName;
import io.lakestream.ursa.lakehouse.v2.AbstractLakehouseWriter;
import io.lakestream.ursa.lakehouse.v2.IWriteResult;
import io.lakestream.ursa.materialization.serde.EntrySerdeFactory;
import io.lakestream.ursa.materialization.serde.GenericEntry;
import io.lakestream.ursa.materialization.serde.MaterializationRecord;
import io.lakestream.ursa.materialization.serde.TableSchemaService;
import io.lakestream.ursa.metrics.InstrumentProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

@Slf4j
public class DeltaExternalTableWriter extends AbstractLakehouseWriter {

    private final String parentTopic;
    private final LakehouseConfiguration config;
    private final List<String> partitionKeys;
    @VisibleForTesting
    @Getter
    private ExternalDeltaTable deltaTable;
    private Map<Integer, ParquetRowWriter> parquetRowWriterMap;
    private boolean isSchemaEvolutionEnabled;
    private DeltaTableSchemaService deltaTableSchemaService;
    private boolean createTable;
    protected final EntrySerdeFactory.SerdeType serializeType;

    @VisibleForTesting
    public DeltaExternalTableWriter(String topic, EntrySerdeFactory entrySerdeFactory,
                                      LakehouseConfiguration configuration) {
        this(topic, entrySerdeFactory, configuration, InstrumentProvider.NOOP);
    }

    public DeltaExternalTableWriter(String topic, EntrySerdeFactory entrySerdeFactory,
                                      LakehouseConfiguration config, InstrumentProvider provider) {
        this(topic, topic, entrySerdeFactory, config, provider);
    }

    public DeltaExternalTableWriter(String topic, String schemaTopic,
                                      EntrySerdeFactory entrySerdeFactory,
                                      LakehouseConfiguration config, InstrumentProvider provider) {
        super(topic, schemaTopic, entrySerdeFactory, config, provider);
        this.config = config;
        this.serializeType = getSerializeType();
        this.encoder = entrySerdeFactory.getEncoder(serializeType);
        String partitionKey = config.getPartitionKey();
        if (StringUtils.isBlank(partitionKey) || LakehouseConfiguration.NONE_PARTITION_KEY.equals(partitionKey)) {
            partitionKeys = Collections.emptyList();
        } else {
            partitionKeys = Arrays.asList(Arrays.stream(partitionKey.split(","))
                .map(String::strip).toArray(String[]::new));
        }
        this.parentTopic = TopicName.get(topic).getPartitionedTopicName();
        this.deltaTable = ExternalDeltaTableFactory.getDeltaTable(config, parentTopic);
        this.isSchemaEvolutionEnabled = config.isSchemaEvolutionEnabled();
        if (isSchemaEvolutionEnabled) {
            this.deltaTableSchemaService = new DeltaTableSchemaService(deltaTable);
        }
        this.parquetRowWriterMap = new HashMap<>();
    }

    private EntrySerdeFactory.SerdeType getSerializeType() {
        return EntrySerdeFactory.SerdeType.KAFKA_DELTA;
    }

    @Override
    protected void beforeWrite(MaterializationRecord<Object> entry) throws LakehouseOptException {
        GenericRow record = (GenericRow) entry.record();
        StructType deltaSchema = record.getSchema();
        Long schemaVersion = null;
        if (entry.metadata().isPresent()) {
            schemaVersion = entry.metadata().get().getSchemaVersion();
            // When the schema is primitive, use the established primitive sentinel.
            schemaVersion = schemaVersion == null ? -2L : schemaVersion;
        }
        initializeDeltaWriter(schemaVersion, deltaSchema);
    }

    @Override
    protected void doWrite(AtomicBoolean writeNumberOfMessagesForThisEntry, GenericEntry genericEntry,
                           MaterializationRecord<Object> objectLakehouseEntry, long writeStartTime)
        throws LakehouseOptException, MessageSerDeException {
        GenericRow record = (GenericRow) objectLakehouseEntry.record();
        try {
            ParquetRowWriter parquetRowWriter =
                parquetRowWriterMap.computeIfAbsent(record.getSchema().hashCode(), k -> {
                    String tableLocation = deltaTable.getTableLocation();
                    Configuration externalHadoopConfig = deltaTable.getTableHadoopConfiguration();
                    return new ParquetRowWriter(tableLocation, externalHadoopConfig, partitionKeys,
                        record.getSchema(), config.getDeltaKernelWriteBatchSize());
                });
            parquetRowWriter.write(record);
        } catch (IOException e) {
            throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_WRITE_ERROR, e);
        }
    }

    private void initializeDeltaWriter(Long schemaVersion, StructType deltaSchema) throws LakehouseOptException {
        try {
            if (!createTable) {
                if (!this.deltaTable.tableExists()) {
                    this.deltaTable.createDeltaTable(schemaVersion, deltaSchema);
                }
                createTable = true;
            }
        } catch (Throwable throwable) {
            String msg = "Failed to init delta table for topic: " + topic;
            log.error(msg, throwable);
            throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_CREATE_TABLE_ERROR, msg, throwable);
        }
    }

    @Override
    public TableSchemaService getLakehouseTableSchemaService() {
        return deltaTableSchemaService;
    }

    @Override
    public List<IWriteResult> close() throws LakehouseOptException {
        logCloseWarnings();
        if (parquetRowWriterMap.isEmpty()) {
            log.warn("No data written to delta table for topic: {}, partition: {}", topic, partition);
            return Collections.emptyList();
        }
        List<IWriteResult> result = new ArrayList<>();
        try {
            for (ParquetRowWriter parquetRowWriter : parquetRowWriterMap.values()) {
                result.add(new DeltaWriteResult(parquetRowWriter.close()));
            }
            return result;
        } catch (IOException e) {
            log.error("Failed to close delta writer", e);
            throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_WRITE_ERROR, "Failed to close delta writer", e);
        }
    }

    @Deprecated
    public ExternalDeltaTable getUcTable() {
        return deltaTable;
    }
}
