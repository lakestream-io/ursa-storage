/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Tag("lakehouse")
public class UnpartitionedDeltaWriterTest extends BaseWriterTest {

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testUnpartitionedDeltaWriter(String format) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.isUpsertModeEnabled()).thenReturn(true);
    when(config.getWriteProps()).thenReturn(ImmutableMap.of(DEFAULT_FILE_FORMAT, format));

    Record row = GenericRecord.create(SCHEMA);
    row.setField("id", 123L);
    row.setField("data", "hello world!");
    row.setField("id2", 123L);

    WriteResult result = writeTest(ImmutableList.of(row), config, UnpartitionedDeltaWriter.class);

    // in upsert mode, each write is a delete + append, so we'll have 1 data file
    // and 1 delete file
    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.dataFiles()).allMatch(file -> file.format() == FileFormat.fromString(format));
    assertThat(result.deleteFiles()).hasSize(1);
    assertThat(result.deleteFiles())
        .allMatch(file -> file.format() == FileFormat.fromString(format));
  }
}
