/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2;

import static io.lakestream.ursa.lakehouse.v2.LakehouseWriterMetrics.WRITER_CLASS_NAME;
import static io.lakestream.ursa.lakehouse.v2.LakehouseWriterMetrics.WRITER_SERDE_TYPE;

import com.google.common.annotations.VisibleForTesting;
import io.lakestream.ursa.exception.ExceptionWithCode;
import io.lakestream.ursa.exception.LakehouseOptException;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.delta.DeltaTableUtils;
import io.lakestream.ursa.lakehouse.v2.io.parquet.ParquetFileWriter;
import io.lakestream.ursa.materialization.serde.EntrySerdeFactory;
import io.lakestream.ursa.materialization.serde.MaterializationRecord;
import io.lakestream.ursa.metrics.InstrumentProvider;
import io.opentelemetry.api.common.Attributes;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LakehouseWriter extends AbstractLakehouseWriter {

    @VisibleForTesting
    public LakehouseWriter(String topic, EntrySerdeFactory entrySerdeFactory,
                          LakehouseConfiguration configuration) {
        this(topic, entrySerdeFactory, configuration, InstrumentProvider.NOOP);
    }

    public LakehouseWriter(String topic, EntrySerdeFactory entrySerdeFactory,
                                 LakehouseConfiguration configuration, InstrumentProvider provider) {
        this(topic, topic, entrySerdeFactory, configuration, provider);
    }

    public LakehouseWriter(String topic, String schemaTopic, EntrySerdeFactory entrySerdeFactory,
                           LakehouseConfiguration configuration, InstrumentProvider provider) {
        super(topic, schemaTopic, entrySerdeFactory, configuration, provider);
        this.serializeType = getSerializeType();
        this.encoder = entrySerdeFactory.getEncoder(serializeType);
        this.writer = new ParquetFileWriter<>(URI.create(
            DeltaTableUtils.generateTableLocation(configuration.getStoragePath(), topic)),
            configuration, provider);
        this.writer.addExtraMetadataAtFile(SERDETYPE, serializeType.name());
        this.attributes =  Attributes.builder()
            .put(WRITER_CLASS_NAME, this.getClass().getSimpleName())
            .put(WRITER_SERDE_TYPE, serializeType.name())
            .build();
    }

    private EntrySerdeFactory.SerdeType getSerializeType() {
        return getValidUrsaSerdeType();
    }

    private EntrySerdeFactory.SerdeType getValidUrsaSerdeType() {
        var serdeType = EntrySerdeFactory.SerdeType.valueOf(configuration.getProperties()
            .getProperty(SERDETYPE, EntrySerdeFactory.SerdeType.KAFKA_BATCHED_RAW_PARQUET.name())
            .toUpperCase(Locale.ROOT));
        switch (serdeType) {
            case KAFKA_PARQUET:
            case KAFKA_BATCHED_RAW_PARQUET:
                return serdeType;
            default:
                throw new IllegalArgumentException("Invalid serde type for ursa cluster: " + serdeType
                                                   + ". Valid options: [kafka_parquet, kafka_batched_raw_parquet]");
        }
    }

    @Override
    protected void beforeWrite(MaterializationRecord<Object> entry) throws LakehouseOptException {
        // do nothing
    }

    @Override
    public List<IWriteResult> close() throws ExceptionWithCode {
        return super.close();
    }
}
