/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.iceberg;

import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.LakehouseOptException;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.compact.FailureMessage;
import io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig;
import io.lakestream.ursa.lakehouse.iceberg.IcebergTable;
import io.lakestream.ursa.lakehouse.iceberg.TableOptions;
import io.lakestream.ursa.lakehouse.iceberg.Utilities;
import io.lakestream.ursa.lakehouse.utils.TopicName;
import io.lakestream.ursa.lakehouse.v2.IWriteResult;
import io.lakestream.ursa.lakehouse.v2.LakehouseRecordWriter;
import io.lakestream.ursa.lakehouse.v2.LakehouseWriterMetrics;
import io.lakestream.ursa.metrics.InstrumentProvider;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;


public class IcebergExternalDLTTableWriter implements LakehouseRecordWriter<FailureMessage> {

    private final String topic;
    private final LakehouseConfiguration configuration;
    // TODO: add metrics later
    private final LakehouseWriterMetrics metrics;
    private final IcebergSinkConfig icebergSinkConfig;
    private TaskWriter<Record> taskWriter;
    public static final Schema ICEBERG_SCHEMA = new Schema(
        Types.NestedField.required(1, "messageId", Types.StringType.get()),
        Types.NestedField.optional(2, "payload", Types.StringType.get()),
        Types.NestedField.optional(3, "failureReason", Types.StringType.get())
    );

    private static final GenericRecord RECORD_TEMPLATE = GenericRecord.create(ICEBERG_SCHEMA);
    private IcebergTable icebergTable;

    public IcebergExternalDLTTableWriter(String topic, LakehouseConfiguration config, InstrumentProvider provider) {
        this.topic = topic;
        this.configuration = config;
        this.metrics = LakehouseWriterMetrics.getInstance(provider);
        this.icebergSinkConfig = new IcebergSinkConfig(config.getProperties());
    }

    @Override
    public void write(FailureMessage entry) throws LakehouseOptException {
        beforeWrite();
        doWrite(entry);
    }

    protected void beforeWrite() throws LakehouseOptException {
        try {
            if (icebergTable == null) {
                icebergTable = initIcebergTable(ICEBERG_SCHEMA, topic, configuration);
            }
            icebergTable.createIfAbsent();
        } catch (Throwable throwable) {
            throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_CREATE_TABLE_ERROR, throwable);
        }

    }

    protected void doWrite(FailureMessage entry) throws LakehouseOptException {
        Record record = toRecord(entry);
        try {
            if (taskWriter == null) {
                taskWriter = Utilities.createTableWriter(icebergTable.getTable(), ICEBERG_SCHEMA, 0, icebergSinkConfig);
            }
            taskWriter.write(record);
        } catch (Throwable t) {
            throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_WRITE_ERROR, t);
        }
    }

    public List<IWriteResult> close() throws LakehouseOptException {
        try {
            if (taskWriter != null) {
                WriteResult writeResult = taskWriter.complete();
                if (writeResult == null || (writeResult.dataFiles().length == 0
                    && writeResult.deleteFiles().length == 0)) {
                    return Collections.emptyList();
                }
                return Collections.singletonList(new IcebergWriteResult(writeResult));
            }
            return Collections.emptyList();
        } catch (Throwable t) {
            throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_WRITE_ERROR, t);
        } finally {
            if (icebergTable != null) {
                icebergTable.close();
                icebergTable = null;
            }
            if (taskWriter != null) {
                taskWriter = null;
            }
        }
    }

    public static Record toRecord(FailureMessage msg) {
        Record record = RECORD_TEMPLATE.copy();
        record.setField("messageId", msg.getMessageId());
        record.setField("payload", msg.getEncodedPayload());
        record.setField("failureReason", msg.getFailureReason());
        return record;
    }

    private static IcebergTable initIcebergTable(Schema schema, String topic, LakehouseConfiguration config) {
        String parentTopic = TopicName.get(topic).getPartitionedTopicName();
        TopicName topicName = TopicName.get(parentTopic);

        var builder = TableOptions.builder();
        builder.schema(schema);
        builder.properties(config.getIcebergTableProperties());
        TableOptions tableOptions = builder.build();

        Namespace namespace = Namespace.of(topicName.getNamespace());
        String tableName = topicName.getLocalName() + config.getDltSuffix();
        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
        return new IcebergTable(config, tableOptions, identifier);
    }
}
