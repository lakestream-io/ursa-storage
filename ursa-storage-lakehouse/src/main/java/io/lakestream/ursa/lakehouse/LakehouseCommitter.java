/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse;

import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.lakehouse.exception.LakehouseException;
import io.lakestream.ursa.lakehouse.utils.TopicName;
import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;

public interface LakehouseCommitter extends AutoCloseable {

    static List<LakehouseCommitter> get(LakehouseConfiguration configuration, String topic) {
        String parentTopic = TopicName.get(topic).getPartitionedTopicName();
        switch (configuration.getLakehouseType()) {
            case ICEBERG:
                return List.of(new IcebergCommitter(configuration, parentTopic));
            case DELTA:
                return List.of(new DeltaCommitter(configuration, parentTopic));
            case DELTA_AND_ICEBERG:
                return List.of(new DeltaCommitter(configuration, parentTopic),
                    new IcebergCommitter(configuration, parentTopic));
            case NONE:
                return List.of();
            default:
                throw new IllegalArgumentException("Unsupported lakehouse type: " + configuration.getLakehouseType());
        }
    }

    boolean tableExists()throws LakehouseException;

    void createTable(Schema schema) throws LakehouseException;

    boolean isTheCompactStreamTaskCommitted(CompactStreamTask compactStreamTask) throws IOException;

    long commit(List<ParquetFileStat> fileStats) throws LakehouseException;

    /**
     * Deletes the specified parquet files from the LakeHouse.
     *
     * <p>This method is responsible for committing the deletion of parquet files
     * to the LakeHouse, preventing users from accessing them and avoiding potential exceptions
     * related to missing files. The operation should be idempotent, allowing for
     * safe retries in case of failures.</p>
     *
     * @param fileStats a list of {@link ParquetFileStat} objects representing the
     *                  files to be deleted.
     * @throws LakehouseException if an error occurs during the deletion process.
     */
    void delete(List<ParquetFileStat> fileStats) throws LakehouseException;

    void updateTablePropertiesIfNeeded(Map<String, String> properties) throws LakehouseException;

    String getName();
}
