/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Set;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.TypeUtil;

abstract class BaseDeltaTaskWriter extends BaseTaskWriter<Record> {

  private final Schema schema;
  private final Schema deleteSchema;
  private final InternalRecordWrapper wrapper;
  private final InternalRecordWrapper keyWrapper;
  private final RecordProjection keyProjection;
  private final boolean upsertMode;

  BaseDeltaTaskWriter(
      PartitionSpec spec,
      FileFormat format,
      FileAppenderFactory<Record> appenderFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize,
      Schema schema,
      Set<Integer> identifierFieldIds,
      boolean upsertMode) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
    this.schema = schema;
    this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(identifierFieldIds));
    this.wrapper = new InternalRecordWrapper(schema.asStruct());
    this.keyWrapper = new InternalRecordWrapper(deleteSchema.asStruct());
    this.keyProjection = RecordProjection.create(schema, deleteSchema);
    this.upsertMode = upsertMode;
  }

  abstract RowDataDeltaWriter route(Record row);

  InternalRecordWrapper wrapper() {
    return wrapper;
  }

  @Override
  public void write(Record row) throws IOException {
    Operation op =
        row instanceof RecordWrapper
            ? ((RecordWrapper) row).op()
            : upsertMode ? Operation.UPDATE : Operation.INSERT;
    RowDataDeltaWriter writer = route(row);
    if (op == Operation.UPDATE || op == Operation.DELETE) {
      writer.deleteKey(keyProjection.wrap(row));
    }
    if (op == Operation.UPDATE || op == Operation.INSERT) {
      writer.write(row);
    }
  }

  class RowDataDeltaWriter extends BaseEqualityDeltaWriter {

    RowDataDeltaWriter(PartitionKey partition) {
      super(partition, schema, deleteSchema);
    }

    @Override
    protected StructLike asStructLike(Record data) {
      return wrapper.wrap(data);
    }

    @Override
    protected StructLike asStructLikeKey(Record data) {
      return keyWrapper.wrap(data);
    }
  }
}
