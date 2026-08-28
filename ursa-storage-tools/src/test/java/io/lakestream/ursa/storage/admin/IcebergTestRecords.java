/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.admin;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;

/**
 * Test helper that writes real Parquet records and reads them back, using the table's own
 * {@code FileIO}. This works against both a local Hadoop catalog and a remote REST catalog
 * (e.g. Snowflake Open Catalog) backed by object storage, so the same verification can be used
 * in unit tests and integration tests.
 *
 * <p>Assumes the table schema has an {@code int} field named {@code id} and a {@code string}
 * field named {@code data}.
 */
final class IcebergTestRecords {

    private IcebergTestRecords() {
    }

    /**
     * Writes one Parquet data file containing a record per supplied id (with {@code data="v<id>"}),
     * placed under the table's {@code data/} location with a random file name, and returns the
     * resulting {@link DataFile}. The file is written but not committed.
     */
    static DataFile writeRecords(Table table, int... ids) throws IOException {
        String filepath = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
        OutputFile outputFile = table.io().newOutputFile(filepath);
        DataWriter<GenericRecord> dataWriter = Parquet.writeData(outputFile)
                .schema(table.schema())
                .createWriterFunc(GenericParquetWriter::create)
                .overwrite()
                .withSpec(PartitionSpec.unpartitioned())
                .build();
        try {
            GenericRecord record = GenericRecord.create(table.schema());
            for (int id : ids) {
                dataWriter.write((GenericRecord) record.copy("id", id, "data", "v" + id));
            }
        } finally {
            dataWriter.close();
        }
        return dataWriter.toDataFile();
    }

    /** Reads every row in the table and returns the {@code id} column values in scan order. */
    static List<Integer> readIds(Table table) {
        List<Integer> ids = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record record : records) {
                ids.add((Integer) record.getField("id"));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read records from " + table.name(), e);
        }
        return ids;
    }
}
