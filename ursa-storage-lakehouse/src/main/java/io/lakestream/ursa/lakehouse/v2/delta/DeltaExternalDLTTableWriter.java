/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.delta;

import com.google.common.annotations.VisibleForTesting;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.LakehouseOptException;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.compact.FailureMessage;
import io.lakestream.ursa.lakehouse.delta.ExternalDeltaTable;
import io.lakestream.ursa.lakehouse.delta.ExternalDeltaTableFactory;
import io.lakestream.ursa.lakehouse.delta.GenericRow;
import io.lakestream.ursa.lakehouse.delta.ParquetRowWriter;
import io.lakestream.ursa.lakehouse.utils.StreamTableNaming;
import io.lakestream.ursa.lakehouse.v2.IWriteResult;
import io.lakestream.ursa.lakehouse.v2.LakehouseRecordWriter;
import io.lakestream.ursa.lakehouse.v2.LakehouseWriterMetrics;
import io.lakestream.ursa.metrics.InstrumentProvider;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

@Slf4j
public class DeltaExternalDLTTableWriter implements LakehouseRecordWriter<FailureMessage> {

    private final String topic;
    private final LakehouseConfiguration config;
    //TODO: add metrics later
    private final LakehouseWriterMetrics metrics;
    @VisibleForTesting
    @Getter
    private ExternalDeltaTable deltaTable;
    private boolean tableInitialized = false;
    private ParquetRowWriter parquetRowWriter;

    public static final StructType DELTA_SCHEMA = new StructType()
        .add("messageId", StringType.STRING)  // ordinal 0
        .add("payload", StringType.STRING)  // ordinal 1
        .add("failureReason", StringType.STRING);  // ordinal 2

    public DeltaExternalDLTTableWriter(String topic, LakehouseConfiguration config, InstrumentProvider provider) {
        this.topic = topic;
        this.config = config;
        this.metrics = LakehouseWriterMetrics.getInstance(provider);
        var mainIdentifier = StreamTableNaming.resolveForWriter(topic, config.getProperties());
        String dltTopic = StreamTableNaming.qualifiedName(
                StreamTableNaming.deadLetterTable(mainIdentifier, config.getDltSuffix()));
        this.deltaTable = ExternalDeltaTableFactory.getDeltaTable(config, dltTopic);
    }

    protected void beforeWrite() throws LakehouseOptException {
        initializeTable(DELTA_SCHEMA);
    }

    protected void doWrite(FailureMessage entry) throws LakehouseOptException {
        // No-op for now
        GenericRow record = toGenericRow(entry);
        try {
            initializeDeltaWriter(DELTA_SCHEMA);
            parquetRowWriter.write(record);
        } catch (Throwable e) {
            throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_WRITE_ERROR, e);
        }
    }

    @Override
    public void write(FailureMessage entry) throws LakehouseOptException {
        beforeWrite();
        doWrite(entry);
    }

    @Override
    public List<IWriteResult> close() throws LakehouseOptException {
        try {
            if (parquetRowWriter == null) {
                return Collections.emptyList();
            }

            return Collections.singletonList(new DeltaWriteResult(parquetRowWriter.close()));
        } catch (Throwable t) {
            throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_WRITE_ERROR, t);
        } finally {
            parquetRowWriter = null;
            tableInitialized = false;
        }
    }

    private void initializeTable(StructType deltaSchema) throws LakehouseOptException {
        try {
            if (!tableInitialized) {
                if (!this.deltaTable.tableExists()) {
                    this.deltaTable.createDeltaTable(null, deltaSchema);
                }
                tableInitialized = true;
            }
        } catch (Throwable throwable) {
            String msg = "Failed to init delta table for topic: " + topic;
            log.error(msg, throwable);
            throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_CREATE_TABLE_ERROR, msg, throwable);
        }
    }

    private void initializeDeltaWriter(StructType deltaSchema) {
        if (parquetRowWriter == null) {
            String tableLocation = deltaTable.getTableLocation();
            Configuration externalHadoopConfig = deltaTable.getTableHadoopConfiguration();
            parquetRowWriter = new ParquetRowWriter(tableLocation, externalHadoopConfig, List.of(), deltaSchema,
                config.getDeltaKernelWriteBatchSize());
        }
    }

    public static GenericRow toGenericRow(FailureMessage msg) {
        Map<Integer, Object> ordinalToValue = new HashMap<>();
        ordinalToValue.put(0, msg.getMessageId());       // messageId
        ordinalToValue.put(1, msg.getEncodedPayload());  // payload
        ordinalToValue.put(2, msg.getFailureReason());   // failureReason

        return new GenericRow(DELTA_SCHEMA, ordinalToValue);
    }

    @Deprecated
    public ExternalDeltaTable getUcTable() {
        return deltaTable;
    }
}
