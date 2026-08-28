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
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Tag("lakehouse")
public class UnpartitionedWriterTest extends BaseWriterTest {

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testUnpartitionedWriter(String format) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.getWriteProps()).thenReturn(ImmutableMap.of(DEFAULT_FILE_FORMAT, format));

    Record row1 = GenericRecord.create(SCHEMA);
    row1.setField("id", 123L);
    row1.setField("data", "hello world!");
    row1.setField("id2", 123L);

    Record row2 = GenericRecord.create(SCHEMA);
    row2.setField("id", 234L);
    row2.setField("data", "foobar");
    row2.setField("id2", 234L);

    WriteResult result = writeTest(ImmutableList.of(row1, row2), config, UnpartitionedWriter.class);

    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.dataFiles()).allMatch(file -> file.format() == FileFormat.fromString(format));
    assertThat(result.deleteFiles()).hasSize(0);
  }
}
