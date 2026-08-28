/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import io.delta.kernel.data.Row;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

@Slf4j
public abstract class ExternalDeltaTable extends DeltaTable {

    protected ExternalDeltaTable(LakehouseConfiguration config, String parentTopic) {
        super(config, parentTopic);
    }

    @Override
    public List<Row> buildAddFileAction(List<ParquetFileStat> fileStats) {
        List<Row> rows = new ArrayList<>();
        for (ParquetFileStat fileStatWrapper : fileStats) {
            for (ParquetFileStat fileStat : fileStatWrapper.getDeltaFiles()) {
                log.info("add filePath: {}, partitionValues: {}, fileSize: {}", fileStat.getFilePath(),
                    fileStat.getPartitionValues(), fileStat.getFileSize());
                Map<String, String> filteredTags;
                if (fileStat.getTags() != null) {
                    filteredTags = fileStat.getTags().entrySet()
                        .stream()
                        .filter(entry -> TAG_KEYS.contains(entry.getKey()))
                        .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue));
                } else {
                    filteredTags = new HashMap<>();
                }
                filteredTags.put(ORDER_TAG, "true");
                Row addFileRow = DeltaTableUtils.buildAddFileAction(fileStat.getFilePath(), fileStat.getFileSize(),
                    System.currentTimeMillis(), fileStat.getPartitionValues(), true, fileStat.getStats(),
                    filteredTags);
                rows.add(addFileRow);
            }
        }
        return rows;
    }

    public abstract Configuration getTableHadoopConfiguration();
}
