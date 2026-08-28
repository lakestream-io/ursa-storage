/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.io.parquet;

import io.lakestream.ursa.lakehouse.v2.IWriteResult;
import java.net.URI;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

@AllArgsConstructor
@Getter
public class ParquetWriteResult implements IWriteResult {
    URI directory;
    String dataFile;
    long dataFileSize;
    long numberOfRecords;
    String indexFile;
    ParquetMetadata metadata;
    Map<String, Object> extraMetadata;
}
