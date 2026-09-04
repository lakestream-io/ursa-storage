/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.iceberg;

import static io.lakestream.ursa.lakehouse.v2.LakehouseWriterMetrics.WRITER_CLASS_NAME;
import static io.lakestream.ursa.lakehouse.v2.LakehouseWriterMetrics.WRITER_SERDE_TYPE;

import io.lakestream.ursa.exception.ExceptionCode;
import io.lakestream.ursa.exception.ExceptionWithCode;
import io.lakestream.ursa.exception.LakehouseOptException;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.iceberg.IcebergTable;
import io.lakestream.ursa.lakehouse.iceberg.TableOptions;
import io.lakestream.ursa.lakehouse.utils.AvroSchemaUtilExtended;
import io.lakestream.ursa.lakehouse.v2.IWriteResult;
import io.lakestream.ursa.lakehouse.v2.LakehouseWriter;
import io.lakestream.ursa.materialization.serde.EntrySerdeFactory;
import io.lakestream.ursa.materialization.serde.MaterializationRecord;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.opentelemetry.api.common.Attributes;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

@Slf4j
public class IcebergManagedTableWriter extends LakehouseWriter {

    private boolean isTableCreated = false;
    private IcebergTable icebergTable;

    public IcebergManagedTableWriter(String topic, EntrySerdeFactory entrySerdeFactory,
                                     LakehouseConfiguration configuration, InstrumentProvider provider) {
        this(topic, topic, entrySerdeFactory, configuration, provider);
    }

    public IcebergManagedTableWriter(String topic, String schemaTopic,
                                     EntrySerdeFactory entrySerdeFactory,
                                     LakehouseConfiguration configuration, InstrumentProvider provider) {
        super(topic, schemaTopic, entrySerdeFactory, configuration, provider);
        this.attributes =  Attributes.builder()
            .put(WRITER_CLASS_NAME, this.getClass().getSimpleName())
            .put(WRITER_SERDE_TYPE, serializeType.name()).build();
    }

    @Override
    protected void beforeWrite(MaterializationRecord<Object> entry) throws LakehouseOptException {
        super.beforeWrite(entry);
        try {

            if (this.icebergTable == null) {
                if (entry.record() instanceof GenericRecord genericRecord) {
                    this.icebergTable = getIcebergTable(genericRecord.getSchema());
                } else {
                    throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_WRITE_ERROR,
                        "Invalid records written to the iceberg. Only Avro record is supported");
                }
            }
            if (!isTableCreated) {
                this.icebergTable.createIfAbsent();
                isTableCreated = true;
            }
        } catch (Throwable throwable) {
            if (throwable instanceof LakehouseOptException loe) {
                throw loe;
            } else {
                String msg = "Failed to init iceberg table for topic: " + topic;
                log.error(msg, throwable);
                throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_CREATE_TABLE_ERROR, msg, throwable);
            }
        }
    }

    private IcebergTable getIcebergTable(Schema schema) throws LakehouseOptException {
        try {
            var tableIdentifier = IcebergTable.getTableIdentifierByTopic(topic, configuration);

            // Check if Unity Catalog is being used
            boolean isUnityCatalog = IcebergTable.ICEBERG_CATALOG_TYPE_UNITYCATALOG.equalsIgnoreCase(
                configuration.getIcebergCatalogBackendType(configuration.getCatalogName()).toString());

            org.apache.iceberg.Schema icebergSchema = AvroSchemaUtilExtended.toIceberg(
                schema, configuration.isAllowIcebergV3(), isUnityCatalog);
            TableOptions options = TableOptions.builder()
                .schema(icebergSchema)
                .properties(configuration.getIcebergTableProperties())
                .build();
            return new IcebergTable(configuration, options, tableIdentifier);
        } catch (Throwable throwable) {
            throw new LakehouseOptException(ExceptionCode.LAKEHOUSE_CREATE_TABLE_ERROR,
                "Failed to create iceberg table for topic: " + topic, throwable);
        }
    }

    @Override
    public List<IWriteResult> close() throws ExceptionWithCode {
        try {
            return super.close();
        } finally {
            if (icebergTable != null) {
                icebergTable.close();
                icebergTable = null;
            }
        }
    }
}
