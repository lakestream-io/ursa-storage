/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class AvroRecordMaterializer extends RecordMaterializer<GenericRecord> {

    private final AvroRecordConverter rootConverter;

    public AvroRecordMaterializer(MessageType parquetSchema, Schema avroSchema) {
        this.rootConverter = new AvroRecordConverter(parquetSchema, avroSchema);
    }

    @Override
    public GenericRecord getCurrentRecord() {
        return rootConverter.getCurrentRecord();
    }

    @Override
    public AvroRecordConverter getRootConverter() {
        return rootConverter;
    }
}
