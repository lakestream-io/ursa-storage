/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;

public class BaseWriterTest {

  protected InMemoryFileIO fileIO;
  protected Table table;

  protected static final Schema SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "data", Types.StringType.get()),
              Types.NestedField.required(3, "id2", Types.LongType.get())),
          ImmutableSet.of(1, 3));

  protected static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("data").build();

  @BeforeEach
  public void before() {
    fileIO = new InMemoryFileIO();

    table = mock(Table.class);
    when(table.schema()).thenReturn(SCHEMA);
    when(table.spec()).thenReturn(PartitionSpec.unpartitioned());
    when(table.io()).thenReturn(fileIO);
    when(table.locationProvider())
        .thenReturn(LocationProviders.locationsFor("file", ImmutableMap.of()));
    when(table.encryption()).thenReturn(PlaintextEncryptionManager.instance());
    when(table.properties()).thenReturn(ImmutableMap.of());
  }

  protected WriteResult writeTest(
      List<Record> rows, IcebergSinkConfig config, Class<?> expectedWriterClass) {
    try (TaskWriter<Record> writer = Utilities.createTableWriter(table, table.schema(), 0, config)) {
      assertThat(writer.getClass()).isEqualTo(expectedWriterClass);

      rows.forEach(
          row -> {
            try {
              writer.write(row);
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          });

      return writer.complete();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
