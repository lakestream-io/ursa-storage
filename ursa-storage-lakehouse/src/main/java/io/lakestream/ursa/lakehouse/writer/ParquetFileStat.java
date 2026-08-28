/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.writer;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.WriteResult;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class ParquetFileStat implements Serializable {
    @Serial
    private static final long serialVersionUID = 3191230871658919901L;

    protected String filePath;
    protected String fileFullPath;
    protected Long fileSize;
    protected Map<String, String> partitionValues;
    protected Map<String, String> tags;
    protected String stats;

    // Iceberg WriteResult for external table
    @Deprecated
    protected WriteResult writeResult;
    protected List<ParquetFileStat> deltaFiles;
    protected List<WriteResult> writeResults;

    public static ParquetFileStat fromDeltaFiles(List<ParquetFileStat> deltaFiles, Map<String, String> tags) {
        ParquetFileStat stat = new ParquetFileStat();
        stat.deltaFiles = deltaFiles;
        stat.tags = tags;
        return stat;
    }

    public static ParquetFileStat fromWriteResults(List<WriteResult> writeResults, Map<String, String> tags) {
        ParquetFileStat stat = new ParquetFileStat();
        stat.writeResults = writeResults;
        stat.tags = tags;
        return stat;
    }

    public ParquetFileStat(String filePath,
                           String fileFullPath,
                           Long fileSize, String stats,
                           Map<String, String> partitionValues,
                           Map<String, String> tags) {
        this.filePath = filePath;
        this.fileFullPath = fileFullPath;
        this.fileSize = fileSize;
        this.stats = stats;
        this.partitionValues = partitionValues;
        this.tags = tags;
    }

    public DataFile toDataFile(Table table) {
        return DataFiles.builder(table.spec())
            .withPath(fileFullPath)
            .withFileSizeInBytes(fileSize)
            .withFormat(FileFormat.PARQUET)
            .withRecordCount(Long.parseLong(tags.get("totalMessage")))
            .build();
    }
}
